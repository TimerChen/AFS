#ifndef GFS_TEST_PIPESTREAM_HPP
#define GFS_TEST_PIPESTREAM_HPP

#include "common_headers.h"

class pipe_device_impl
{
public:
	typedef char char_type;
	typedef boost::iostreams::bidirectional_device_tag category;

	pipe_device_impl() : is_open(true) {}

	std::streamsize read(char *s, std::streamsize n)
	{
		std::unique_lock<std::mutex> lock(mtxBuffer);
		for (std::streamsize i = 0; i < n; i++)
		{
			while (buffer.empty())
			{
				if (i != 0 || !is_open)
					return i;
				cvBuffer.wait(lock);
			}
			s[i] = buffer.front();
			buffer.pop_front();
		}
		return n;
	}

	std::streamsize write(const char *s, std::streamsize n)
	{
		std::lock_guard<std::mutex> lock(mtxBuffer);
		for (std::streamsize i = 0; i < n; i++)
			buffer.push_back(s[i]);
		cvBuffer.notify_one();
		return n;
	}

	void close()
	{
		std::lock_guard<std::mutex> lock(mtxBuffer);
		is_open = false;
	}

protected:
	bool					is_open;
	std::deque<char>		buffer;
	std::mutex				mtxBuffer;
	std::condition_variable	cvBuffer;
};

class pipe_device
{
public:
	typedef char char_type;
	typedef boost::iostreams::bidirectional_device_tag category;

public:
	pipe_device() : device(std::make_shared<pipe_device_impl>()) {}

public:
	std::streamsize read(char *s, std::streamsize n)
	{
		return device->read(s, n);
	}
	std::streamsize write(const char *s, std::streamsize n)
	{
		return device->write(s, n);
	}
	void close()
	{
		device->close();
	}

protected:
	std::shared_ptr<pipe_device_impl> device;
};

typedef boost::iostreams::stream<pipe_device> pipe_stream;

#endif