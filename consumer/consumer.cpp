// consumer.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <exception>
#include <string>
#include <sstream>
#include <thread>
#include <mutex>
#include <queue>
#include <time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <opencv2\opencv.hpp>


// exception pointer used to transport exceptions between threads
static std::exception_ptr teptr = nullptr;


typedef struct timeval {
	long tv_sec;
	long tv_usec;
} TIMEVAL, *PTIMEVAL, *LPTIMEVAL;


class EOT : public std::exception
{
public:

	EOT() : exception("EOT")
	{
	}

};


class RabbitMQ_consumer
{
public:

	RabbitMQ_consumer(std::string producer_name, std::string hostname = "localhost", int port = 5672, std::string exchange_name = "my_exchange_1", std::string exchange_type = "direct", std::string routing_key = "my_key_1")
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

		th = thread([this]() {
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

				// get queue name
				amqp_queue_declare_ok_t* r = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
				handle_amqp_response(amqp_get_rpc_reply(conn), "Declaring queue");
				amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);
				if (queuename.bytes == NULL)
					throw std::exception("Out of memory while copying queue name");

				string queuename_str;
				for (size_t k = 0; k < queuename.len; ++k)
					queuename_str += (static_cast<uchar*>(queuename.bytes))[k];

				// bind the consumer to the given queue	
				cout << " [" << name.c_str() << "] Binding with queue: " << queuename_str << endl;
				amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange_name.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
				handle_amqp_response(amqp_get_rpc_reply(conn), "Binding queue");

				cout << " [" << name.c_str() << "] routing key: " << routing_key.c_str() << endl;

				// start the consume process 
				amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
				handle_amqp_response(amqp_get_rpc_reply(conn), "Consuming");

				// setup timeout interval for consume
				struct timeval timeout;
				timeout.tv_sec = 1;
				timeout.tv_usec = 0;

				// wait and receive messages
				cout << " [" << name.c_str() << "] Waiting for messages..." << endl;
				while (true)
				{
					amqp_maybe_release_buffers(conn);

					amqp_envelope_t envelope;

					amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

					if (res.reply_type != AMQP_RESPONSE_NORMAL && 
						!(res.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && res.library_error == AMQP_STATUS_TIMEOUT))
						handle_amqp_response(res, "Consuming messages");

					// scope for lock with RAII
					{
						lock_guard<mutex> guard(mx);

						if (do_exit)
						{
							cout << " [" << name.c_str() << "] Received exit flag" << endl;
							break;
						}
					}
		
					if (res.reply_type == AMQP_RESPONSE_NORMAL)
					{
						cout << " [" << name.c_str() << "] Received message #" << static_cast<unsigned>(envelope.delivery_tag)
							<< " of length " << envelope.message.body.len;
						if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
						{
							cout << " Content-type: ";
							for (size_t k = 0; k < envelope.message.properties.content_type.len; ++k)
								cout << (static_cast<uchar*>(envelope.message.properties.content_type.bytes))[k];
						}
						cout << endl;

						// convert to our buffer structure
						vector<uchar> msg(envelope.message.body.len);
						for (size_t k = 0; k < envelope.message.body.len; ++k)
							msg[k] = (static_cast<uchar*>(envelope.message.body.bytes))[k];

						amqp_destroy_envelope(&envelope);

						cout << " [" << name.c_str() << "] Placing on queue a message of size " << msg.size() << endl;

						// scope for lock with RAII
						{
							lock_guard<mutex> guard(mx);
							msg_queue.push(msg);
						}

						waitrd.notify_one();	// should be outside the lock
					}
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


	void receive(std::vector<uchar>& msg)
	{
		using namespace std;

		// since we can not check whether the thread has finished, we at least rethrow any exceptions that have occurred
		if (teptr)
			rethrow_exception(teptr);

		// wait for something to be placed on the internal queue
		{
			unique_lock<mutex> lock(mx);

			while (msg_queue.size() == 0 && !do_exit)
			{
				waitrd.wait(lock);
			}

			msg = msg_queue.front();
			msg_queue.pop();

			lock.unlock();
		}

		// check if EOT 
		if (msg.size() == 1 && msg[0] == 4)
		{
			cout << " [" << name.c_str() << "] Received EOT" << endl;
			throw EOT();
		}
	}


	std::string receive()
	{
		using namespace std;

		vector<uchar> msg;
		receive(msg);

		string str(msg.begin(), msg.end());

		return str;
	}


	void receive(cv::Mat& img)
	{
		using namespace std;

		vector<uchar> msg;
		receive(msg);

		// decode image
		img = cv::imdecode(cv::Mat(msg), CV_LOAD_IMAGE_UNCHANGED);
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
				amqp_connection_close_t* m = (amqp_connection_close_t*)x.reply.decoded;
				stringstream strm;
				strm << context << ": server connection error " << m->reply_code << ", message: ";
				for (int k = 0; k < m->reply_text.len; ++k)
					strm << static_cast<unsigned char*>(m->reply_text.bytes)[k];
				err = strm.str();
				break;
			}
			case AMQP_CHANNEL_CLOSE_METHOD: {
				amqp_channel_close_t* m = (amqp_channel_close_t*)x.reply.decoded;
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

	RabbitMQ_consumer cons1("cons1", "localhost", 5672, "my_exchange_1", "direct", arg);

	try
	{
		cons1.run();

		if (arg.compare("images") == 0)
		{
			while (true)
			{
				cv::Mat img;
				cons1.receive(img);
				cout << "Received an image of size " << img.cols << "x" << img.rows << endl;

				cv::namedWindow("output");
				cv::imshow("output", img);
				cv::waitKey(10);
			}
		}
		else
		{
			while (true)
			{
				string msg = cons1.receive();
				cout << "Received message of length " << msg.length() << " : " << msg.c_str() << endl;
			}
		}

		this_thread::sleep_for(6s);
	}
	catch (const exception& x)
	{
		cout << "Exception: " << x.what() << endl;
		rc = EXIT_FAILURE;
	}

	cout << "quitting" << endl;
	auto& th = cons1.exit();
	if (th.joinable())
		th.join();

	return rc;
}
