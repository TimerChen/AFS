//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_SERVER_H
#define AFS_SERVER_H

#include "setting.h"

namespace AFS {

class Server {
    ServerAddress address;

protected:
    //Find recover files
    void load();
    //Save server files
    void save();
public:
    Server();

    //Stop server
    void stop();
    //Restart server
    void restart();


};

}


#endif //AFS_SERVER_H
