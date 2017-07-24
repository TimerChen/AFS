#include "client.h"

#include "error.hpp"
#include "master.h"

namespace AFS {


Client::Client()
{

	masterAdd = NULL;

}

void Client::fileCreate	( const std::string &dir ) {
	if( !masterAdd ) throw( MasterNotFind() );
	//call master
	Message::cm_Create msg;
	Message::cm_Create_r rmsg;
	rmsg = masterAdd->call_fileCreate( msg );



}

void Client::fileRead	( const std::string &dir, const std::string &localDir ) {
	if( !masterAdd ) throw( MasterNotFind() );
}

void Client::fileWrite	( const std::string &dir, const std::string &localDir ) {

}

void Client::fileAppend	( const std::string &dir, const std::string &localDir ) {

}

void Client::fileMkdir	( const std::string &dir ) {

}

}
