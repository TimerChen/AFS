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

std::pair<AFS::MasterError, std::unique_ptr<std::vector<std::string>>>
AFS::PathFiledata::listName(const AFS::PathFiledata::Path &path) const {
	auto errMd = getData(path);
	auto result2 = std::make_unique<std::vector<std::string>>();
	if (errMd.first == MasterError::NotExists)
		return std::make_pair(MasterError::NotExists, std::move(result2));
	if (errMd.second.type != Filedata::Type::Folder)
		return std::make_pair(MasterError::NotExists, std::move(result2));
	auto collect_name = [](const Filedata & md)->std::string {
		return md.name;
	};
	result2 = mp.nonsub_collect<std::string>(path, collect_name);
	return std::make_pair(MasterError::OK, std::move(result2));
}

AFS::MasterError AFS::PathFiledata::createFolder(const AFS::PathFiledata::Path &path) {
	Filedata md;
	md.type = Filedata::Type::Folder;
	return ErrTranslator::extrieErrToMasterErr(
			mp.insert(path, std::move(md)));
}
