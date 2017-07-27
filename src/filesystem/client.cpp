#include "client.h"

#include "error.hpp"
#include "master.h"
#include "message.hpp"

namespace AFS {


Client::Client()
{

	masterAdd = NULL;

}

void Client::fileCreate	( const std::string &dir ) {
	if( !masterAdd ) throw( MasterNotFind() );
	//call master
	Message::cmCreate msg;
	Message::cmCreate_r rmsg;
	//rmsg = masterAdd->call_fileCreate( msg );



}

void Client::fileRead	( const std::string &dir, const std::string &localDir ) {
	if( !masterAdd ) throw( MasterNotFind() );

	/*
	//use split

	string s = "Hello,World deep,dark,fantasy,, fantasy";
	cout << s << endl;
	boost::split(vs,s,boost::is_any_of(", "),boost::token_compress_off);

	for( auto i : vs )
		cout << i << endl;

	 */
}

void Client::fileWrite	( const std::string &dir, const std::string &localDir ) {

}

void Client::fileAppend	( const std::string &dir, const std::string &localDir ) {

}

void Client::fileMkdir	( const std::string &dir ) {

}

}