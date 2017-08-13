#ifndef GFS_TEST_COMMON_H
#define GFS_TEST_COMMON_H

#include "common_headers.h"

using AFS::GFSError;
using AFS::GFSErrorCode;

struct TestResult
{
	bool passed;
	std::string description;

	static TestResult Pass(std::string description = "")
	{
		return TestResult{ true, description };
	}
	static TestResult Fail(std::string description)
	{
		return TestResult{ false, description };
	}
};

class TestException : public std::exception
{
public:
	TestException(const TestResult &result) : result(result) {}

public:
	const char *what() const noexcept override
	{
		return result.description.c_str();
	}
	TestResult getTestResult() const noexcept
	{
		return result;
	}

protected:
	TestResult result;
};

struct must_succ_tag {};

static must_succ_tag must_succ;

inline void checkError(const GFSError &gfsError)
{
	if (gfsError.errCode != GFSErrorCode::OK) {
		//throw ;
		throw TestException(TestResult::Fail(
				"GFS Call Failed (code=" + std::to_string(int(gfsError.errCode)) + "): " + gfsError.description));
	}
}

inline void operator|(const GFSError &gfsError, must_succ_tag)
{
	checkError(gfsError);
}

template<typename ...T, size_t ...I>
std::tuple<T...> checkResultImpl(const std::tuple<AFS::GFSError, T...> &result, std::index_sequence<I...>)
{
	checkError(std::get<0>(result));
	return std::make_tuple(std::get<I + 1>(result)...);
}

template<typename T>
T operator|(const std::tuple<GFSError, T> &ret, must_succ_tag)
{
	checkError(std::get<0>(ret));
	return std::get<1>(ret);
}

template<typename ...T>
std::tuple<T...> operator|(const std::tuple<GFSError, T...> &ret, must_succ_tag)
{
	return checkResultImpl(ret, std::make_index_sequence<sizeof...(T)>{});
}

#endif
