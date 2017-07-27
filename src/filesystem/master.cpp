//
// Created by Aaron Ren on 21/07/2017.
//

#include "master.h"

AFS::GFSError AFS::Master::RPCMkdir(std::string path_str) {
	GFSError result;
	try {
		auto path = PathParser::instance().parse(path_str);
		MetadataContainer::Error err = mdc.createFile(*path);
		result.errCode = metadataErrToGFSErr(err);
	} catch(...) {
		result.errCode = GFSErrorCode::UnknownErr;
	}
	return result;
}

std::tuple<AFS::GFSError, std::vector<std::string>> AFS::Master::RPCListFile(std::string path_str) {
	std::tuple<GFSError, std::vector<std::string>> result;
	try {
		auto path = PathParser::instance().parse(path_str);
		auto errPtr = mdc.listName(*path);
		std::get<0>(result).errCode = metadataErrToGFSErr(errPtr.first);
		std::get<1>(result) = *errPtr.second;
	} catch(...) {
		std::get<0>(result) = sorry();
	}
	return result;
}
