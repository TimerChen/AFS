#ifndef SERVERB_H
#define SERVERB_H

#include "service.hpp"

class ServerB
{
public:
	ServerB( LightDS::Service &srv );
	LightDS::Service &srv;
};

#endif // SERVERB_H
