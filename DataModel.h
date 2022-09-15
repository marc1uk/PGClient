#include "Store.h"
#include "PGHelper.h"
#include "PGClient.h"
#include "Logging.h"

class DataModel {
	public:
	DataModel();
	PGHelper postgres_helper;
	PGClient pgclient;
	Store vars;
	Logging* Log;
	zmq::context_t* context = nullptr;
};
