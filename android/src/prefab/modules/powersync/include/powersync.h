#ifndef POWERSYNC_H
#define POWERSYNC_H

#include "sqlite3.h"

extern "C" int sqlite3_powersync_init(sqlite3 *db, char **pzErrMsg,
                           const sqlite3_api_routines *pApi);

#endif
