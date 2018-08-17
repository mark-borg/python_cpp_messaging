// producer.cpp : This file contains the 'main' function. Program execution begins and ends there.
// (c) Mark Borg, August 2018
//

#include <iostream>
#include <exception>
#include <string>
#include <sstream>
#include <thread>
#include <mutex>
#include <queue>
#include <chrono>	// for high_resolution_clock

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <opencv2\opencv.hpp>


// exception pointer used to transport exceptions between threads
static std::exception_ptr teptr = nullptr;


class RabbitMQ_producer
{
public:

	RabbitMQ_producer(std::string producer_name, std::string hostname = "localhost", int port = 5672, std::string exchange_name = "my_exchange_1", std::string exchange_type = "direct", std::string routing_key = "my_key_1")
	{
		this->name = producer_name;
		this->hostname = hostname;
		this->port = port;
		this->exchange_name = exchange_name;
		this->exchange_type = exchange_type;
		this->routing_key = routing_key;
	}


	void run()
	{
		using namespace std;

		th = thread([this](){
			try
			{
				// establish connection...
				cout << " [" << name.c_str() << "] Connecting to RabbitMQ..." << endl;
				amqp_connection_state_t conn = amqp_new_connection();
				amqp_socket_t* socket = amqp_tcp_socket_new(conn);
				if (!socket)
					throw exception("Creating TCP socket");

				handle_response(amqp_socket_open(socket, hostname.c_str(), port), "Opening TCP socket");

				handle_amqp_response(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"), "Logging in");

				amqp_channel_open(conn, 1);
				handle_amqp_response(amqp_get_rpc_reply(conn), "Opening channel");

				cout << " [" << name.c_str() << "] routing key: " << routing_key.c_str() << endl;

				// wait for and send messages
				vector<uchar> msg;
				while (true)
				{
					// wait for something to be placed on the internal queue
					{
						unique_lock<mutex> lock(mx);

						while (msg_queue.size() == 0 && !do_exit)
						{
							waitrd.wait(lock);
						}

						if (do_exit)
						{
							cout << " [" << name.c_str() << "] Received exit flag" << endl;
							break;
						}

						msg = msg_queue.front();
						msg_queue.pop();

						lock.unlock();
					}

					// prepare to send the message
					cout << " [" << name.c_str() << "] Sending a message of length " << msg.size() << endl;
					amqp_bytes_t message_bytes;
					message_bytes.len = msg.size();
					message_bytes.bytes = &(msg[0]); //const_cast<char*>(msg.c_str());

					// send the message
					int rc = amqp_basic_publish(conn, 1,
						amqp_cstring_bytes(exchange_name.c_str()),
						amqp_cstring_bytes(routing_key.c_str()), 1, 0, NULL,
						message_bytes);
					handle_response(rc, "Sending Message");
				}

				// close down
				cout << " [" << name.c_str() << "] Closing down connections..." << endl;
				handle_amqp_response(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
				handle_amqp_response(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
				handle_response(amqp_destroy_connection(conn), "Ending connection");
			}
			catch (const exception& x)
			{
				cout << " [" << name.c_str() << "] A major exception has occurred!! " << x.what() << endl;

				// transport exception between threads
				teptr = current_exception();
			}
			catch (...)
			{
				cout << " [" << name.c_str() << "] A major exception has occurred!!" << endl;

				// transport exception between threads
				teptr = current_exception();
			}

			cout << " [" << name.c_str() << "] finished" << endl;
		});
	}


	void send(const std::vector<uchar>& msg)
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		cout << " [" << name.c_str() << "] Queuing a binary message of size " << msg.size() << endl;

		// scope for lock with RAII
		{
			lock_guard<mutex> guard(mx);
			msg_queue.push(msg);
		}

		waitrd.notify_one();	// should be outside the lock
	}


	void send(const std::string& msg)
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		// convert a string to a vector of chars
		vector<uchar> binary_msg;
		copy(msg.begin(), msg.end(), back_inserter(binary_msg));

		cout << " [" << name.c_str() << "] Queuing a string message of size " << msg.length() << " : " << msg.c_str() << endl;

		// scope for lock with RAII
		{
			lock_guard<mutex> guard(mx);
			msg_queue.push(binary_msg);
		}

		waitrd.notify_one();	// should be outside the lock
	}


	void send(const cv::Mat& img, std::string encoding_format = ".png")
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		if (img.empty())
			throw exception("Error loading image!");

		// convert the image to a binary message
		vector<uchar> img_buffer;
		bool ok = cv::imencode(encoding_format, img, img_buffer);
		if (!ok)
			throw exception("Error encoding image!");

		cout << " [" << name.c_str() << "] Queuing an image of size " << img.cols << "x" << img.rows << endl;

		// scope for lock with RAII
		{
			lock_guard<mutex> guard(mx);
			msg_queue.push(img_buffer);
		}

		waitrd.notify_one();	// should be outside the lock
	}


	void send_EOT()
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		cout << " [" << name.c_str() << "] Queuing EOT" << endl;

		// scope for lock with RAII
		{
			lock_guard<mutex> guard(mx);		// RAII protection for mutex locking
			vector<uchar> msg = { 4 };
			msg_queue.push(msg);
		}

		waitrd.notify_one();	// should be outside the lock
	}


	std::thread& exit()		// returns the thread handle, so that the caller can join it
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		cout << " [" << name.c_str() << "] Setting exit flag & notifying" << endl;

		// scope for lock with RAII
		{
			lock_guard<mutex> guard(mx);		// RAII protection for mutex locking
			do_exit = true;
		}

		waitrd.notify_all();	// should be outside the lock

		return th;
	}


	// wait for queue to flush (all pending messages are sent)
	void flush()
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		cout << " [" << name.c_str() << "] flushing queue" << endl;

		bool queue_is_full = true;
		while (queue_is_full)
		{
			// scope for lock with RAII
			{
				lock_guard<mutex> guard(mx);		// RAII protection for mutex locking
				if (msg_queue.size() == 0)
					queue_is_full = false;
			}
			if (queue_is_full)
				this_thread::sleep_for(100ms);

			if (teptr)
				rethrow_exception(teptr);
		}
	}


	std::thread& flush_and_exit()		// returns the thread handle, so that the caller can join it
	{
		flush();
		return exit();
	}


private:

	void handle_amqp_response(amqp_rpc_reply_t x, std::string context)
	{
		using namespace std;

		string err;

		switch (x.reply_type) {
		case AMQP_RESPONSE_NORMAL:
			return;

		case AMQP_RESPONSE_NONE:
			err = context + ": missing RPC reply type!";
			break;

		case AMQP_RESPONSE_LIBRARY_EXCEPTION:
			err = context + ": " + amqp_error_string2(x.library_error);
			break;

		case AMQP_RESPONSE_SERVER_EXCEPTION:
			switch (x.reply.id) {
			case AMQP_CONNECTION_CLOSE_METHOD: {
				amqp_connection_close_t* m = (amqp_connection_close_t*) x.reply.decoded;
				stringstream strm;
				strm << context << ": server connection error " << m->reply_code << ", message: ";
				for (int k = 0; k < m->reply_text.len; ++k)
					strm << static_cast<unsigned char*>(m->reply_text.bytes)[k];
				err = strm.str();
				break;
			}
			case AMQP_CHANNEL_CLOSE_METHOD: {
				amqp_channel_close_t* m = (amqp_channel_close_t*) x.reply.decoded;
				stringstream strm;
				strm << context << ": server channel error " << m->reply_code << ", message: ";
				for (int k = 0; k < m->reply_text.len; ++k)
					strm << static_cast<unsigned char*>(m->reply_text.bytes)[k];
				err = strm.str();
				break;
			}
			default:
				err = context + ": unknown server error, method id " + to_string(static_cast<int>(x.reply.id));
				break;
			}
			break;
		}

		throw exception(err.c_str());
	}

	void handle_response(int rc, std::string context)
	{
		if (rc < 0)
			throw std::exception(context.c_str());
	}


	int port;
	std::string name;
	std::string hostname;
	std::string exchange_name;
	std::string exchange_type;
	std::string routing_key;

	std::thread th;
	std::queue<std::vector<uchar>> msg_queue;
	std::mutex mx;
	std::condition_variable waitrd;
	bool do_exit = false;
};


int main(int argc, char** argv)
{
	using namespace std;
	using namespace chrono_literals;

	int rc = EXIT_SUCCESS;

	string arg = (argc > 1) ? argv[1] : "text";

	RabbitMQ_producer prod1("prod1", "localhost", 5672, "my_exchange_1", "direct", arg);

	try
	{
		prod1.run();

		if (arg.compare("images") == 0)
		{
			cv::Mat img = cv::imread("..\\rabbitmq-logo.png");
			
			this_thread::sleep_for(2s);
			prod1.send(img);

			this_thread::sleep_for(2s);
			prod1.send(img, ".jpg");

			this_thread::sleep_for(2s);
			prod1.send(img, ".bmp");

			// timings
			auto start = chrono::high_resolution_clock::now();
			for (int k = 0; k < 1000; ++k)
				prod1.send(img, ".jpg");
			prod1.flush();
			auto finish = chrono::high_resolution_clock::now();
			chrono::duration<double> elapsed = finish - start;
			cout << "Elapsed time: " << elapsed.count() << " s" << endl;
		}
		else
		{
			this_thread::sleep_for(2s);
			prod1.send("The time has come, the Walrus said, To talk of many things: Of shoes--and ships--and sealing - wax--, Of cabbages--and kings--, And why the sea is boiling hot--, And whether pigs have wings.");

			this_thread::sleep_for(2s);
			prod1.send("A second string");

			this_thread::sleep_for(2s);
			prod1.send("A third string");

			this_thread::sleep_for(2s);
			prod1.send("A fourth string");

			// timings
			auto start = chrono::high_resolution_clock::now();
			for (int k = 0; k < 1000; ++k)
				prod1.send("Timing test for message throughput");
			prod1.flush();
			auto finish = chrono::high_resolution_clock::now();
			chrono::duration<double> elapsed = finish - start;
			cout << "Elapsed time: " << elapsed.count() << " s" << endl;
		}

		this_thread::sleep_for(2s);
		prod1.send_EOT();				// send end-of-transmission

		this_thread::sleep_for(2s);
	}
	catch (const exception& x)
	{
		cout << "Exception: " << x.what() << endl;
		rc = EXIT_FAILURE;
	}

	cout << "quitting" << endl;
	auto& th = prod1.flush_and_exit();
	if (th.joinable())
		th.join();

	return rc;
}
