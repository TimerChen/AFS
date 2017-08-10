#include "common_headers.h"
#include "graybox_env.h"
#include "graybox_basic.h"

int main()
{
	Environment env;
	BasicTest test(env);
	test.Run();
	return 0;
}