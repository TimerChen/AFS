//
// Created by Aaron Ren on 28/07/2017.
//

#include "path_filedata.h"

bool AFS::PathFileData::checkData(const AFS::PathFileData::Path &path) const {
	return mp.check(path);
}

std::pair<AFS::MasterError, AFS::FileDataCopy>
AFS::PathFileData::getData(const AFS::PathFileData::Path &path) const {
	if (!checkData(path))
		return std::make_pair(MasterError::NotExists, FileDataCopy());
	return std::make_pair(MasterError::OK, FileDataCopy(mp[path]));
}