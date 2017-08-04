#ifndef AFS_CLIENT_H
#define AFS_CLIENT_H

#include <string>
#include <service.hpp>
#include "common.h"
#include "setting.hpp"

//#include "master.h"

namespace AFS {

class Client {
public:
	explicit Client(LightDS::Service &_srv);

	void setMaster(Address add) {
		masterAdd = std::move(add);
		throw ;
		// todo
	}

private:
	Address masterAdd;
	LightDS::Service &srv;

	bool checkMasterAddress() const;;

	ClientErr fileCreate(const std::string &dir);
	void fileRead(const std::string &dir, const std::string &localDir);
	void fileWrite(const std::string &dir, const std::string &localDir);

	ClientErr _fileAppend(const std::string &dir, const std::string & data);
	ClientErr fileAppend(const std::string &dir, const std::string &localDir) {
		// todo 分拆
		/** fileAppend行为如下：
		 * 1. 首先向master询问文件的信息，如果失败则直接返回对应错误信息，否则继续运行。
		 * 2. 根据该文件含chunk个数定idx，并向master询问该chunk的handle
		 *    a. 若成功则继续
		 *    b. 若因NoSuchChunk或Unknown失败，则都返回Unknown，因为这一步不应该找不到。
		 * 3. 向master询问该chunk的各个副本位置，主副本位置等。
		 *    a. 成功则继续，任何类型失败都退出。
		 * 4. 向各个副本推送数据，使用rand()来作为dataID，只要有一个失败，
		 *    就重新推送，重复x次，在之后的重复中，会优先向上一次失败的副本推送。//todo 重复3次？
		 * 5. 完成推送后，会通知主副本应用修改。
		 *    a. 成功则继续，Unknown则退出
		 *    b. 如果返回不再持有租约，则会回到3继续进行，重复3次
		 *    c. 如果返回已满，则++idx，然后回到2，重复3次
		 */
		std::string data, tmp;
		std::ifstream fin(localDir);
		while (fin.peek() != EOF) {
			fin >> tmp;
			data += tmp;
		}
		return _fileAppend(dir, data);
	}

	ClientErr fileMkdir(const std::string &dir);
};

}

#endif // CLIENT_H
