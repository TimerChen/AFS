//
// Created by Aaron Ren on 28/07/2017.
//

#include "path_filedata.h"

bool AFS::PathFiledata::checkData(const AFS::PathFiledata::Path &path) const {
	return mp.check(path);
}

std::pair<AFS::MasterError, AFS::Filedata>
AFS::PathFiledata::getData(const AFS::PathFiledata::Path &path) const {
	if (!checkData(path))
		return std::make_pair(MasterError::NotExists, Filedata());
	return std::make_pair(MasterError::OK, mp[path]);
}