# Sending messages between C++ and Python applications via RabbitMQ

The C++ and Python classes provided here simplify the process of communicating and sending messages between a C++ application and a Python application using **RabbitMQ** (or any other **AMQP compliant messaging system**).
They should be considered more as an example or as a proof-of-concept. Some restructuring is needed, if used in production code. For example, the main method and classes are in the same file; these should be placed in their proper header (.hpp) and source files (.cpp).

The underlying message format is a binary array (vector of unsigned char for C++, and raw binary string for Python). 

In addition, examples are given of how to encode string data, as well as images. I am using **OpenCV**'s image class (cv::Mat) as an example here, but any other image structure will do.

## Requirements

RabbitMQ   (https://www.rabbitmq.com/)
OpenCV   (https://opencv.org/)
Pika     (https://pika.readthedocs.io/en/stable/)
rabbitmq-c   (https://github.com/alanxz/rabbitmq-c)

I am using Visual Studio 2017 for compiling the C++ code (solution file included). 
For the C++ code, static linking is used for the rabbitmq-c library. (Dynamic linking can be configured instead). There are two Visual Studio property sheets (RabbitMQ.props and OpenCV x64.props) that simplify the configuration of the C++ project files - paths will definitely need changing to point to the correct location where rabbitmq-c and OpenCV have been installed.

The command scripts provide shortcuts for starting/stopping the RabbitMQ service, as well as querying the status of RabbitMQ and the active lists. Some changes to the paths may be required.

## Running the tests

This involves running a producer and a consumer - any of the C++ or the Python versions can be used.

There are two test versions in each file: string-based messaging and image-based messaging.
The default test involves string-based messaging. To run the image-based messaging tests, then pass the worrd *images* as argument to either the Python script or the C++ executable.

Possible combinations of tests include:

(1) python to python string-based messaging
python producer.py
python consumer.py

(2) python to python image-based messaging
python producer.py images
python consumer.py images

(3) C++ to C++ string-based messaging
producer.exe 
consumer.exe 

(4) C++ to C++ image-based messaging
producer.exe images
consumer.exe images

(5) python to C++ image-based messaging
python producer.py images
consumer.exe images

(6) C++ to python image-based messaging
producer.exe images
python consumer.py images


## Results

The largest image I ran these tests with had a size of 275.9Mb, and it worked without any problem.

The test code sends 1000 copies of the same image as separate messages. This takes less than 1 second on my machine (equivalent to around 300 fps for an image size of 600x180, 22Kb message size). 


## Using the classes in your applications

An application may actually have multiple consumers and producers running concurrently within it. Thus you might need to include both classes in the same application. The choice of exchange names and routing keys can be modified to allow for more complex messaging. See the RabbitMQ site for more detail about possible messaging architectures.


More detail can be found in [this related blog entry](https://mark-borg.github.io/blog/2018/rabbit_mq_c++_python/).

If this code is used in any way, please credit the author.
