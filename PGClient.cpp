#include "PGClient.h"
#include "DataModel.h"
#include <errno.h>

Query::Query(std::string dbname_in, std::string query_string_in, char type_in){
	dbname = dbname_in;
	query_string = query_string_in;
	type = type_in;
}

Query::Query(const Query& qry_in){
	dbname = qry_in.dbname;
	query_string = qry_in.query_string;
	type = qry_in.type;
	success = qry_in.success;
	query_response = qry_in.query_response;
	err = qry_in.err;
	msg_id = qry_in.msg_id;
}

void PGClient::SetDataModel(DataModel* m_data_in){
	m_data = m_data_in;
}

bool PGClient::Initialise(std::string configfile){
	
	/*               Retrieve Configs            */
	/* ----------------------------------------- */
	
	// configuration options can be parsed via a Store class
	m_variables.Initialise(configfile);
	
	
	/*            General Variables              */
	/* ----------------------------------------- */
	verbosity = 3;
	max_retries = 3;
	m_variables.Get("verbosity",verbosity);
	m_variables.Get("max_retries",max_retries);
	
	get_ok = InitLogging();
	get_ok = InitZMQ();
	if(not get_ok) return false;
	get_ok &= InitServiceDiscovery();
	get_ok &= RegisterServices();
	if(not get_ok) return false;
	
	/*                Time Tracking              */
	/* ----------------------------------------- */
	
	// time to wait between resend attempts if not ack'd
	int resend_period_ms = 1000;
	// how often to print out stats on what we're sending
	int print_stats_period_ms = 5000;
	
	// Update with user-specified values.
	m_variables.Get("resend_period_ms",resend_period_ms);
	m_variables.Get("print_stats_period_ms",print_stats_period_ms);
	
	// convert times to boost for easy handling
	resend_period = boost::posix_time::milliseconds(resend_period_ms);
	print_stats_period = boost::posix_time::milliseconds(print_stats_period_ms);
	
	// initialise 'last send' times
	last_write = boost::posix_time::microsec_clock::universal_time();
	last_read = boost::posix_time::microsec_clock::universal_time();
	last_printout = boost::posix_time::microsec_clock::universal_time();
	
	// get the hostname of this machine for monitoring stats
	char buf[255];
	get_ok = gethostname(buf, 255);
	if(get_ok!=0){
		std::cerr<<"Error getting hostname!"<<std::endl;
		perror("gethostname: ");
		hostname = "unknown";
	} else {
		hostname = std::string(buf);
	}
	
	// kick off a thread to do actual send and receive of messages
	std::future<void> signal = terminator.get_future();
	background_thread = std::thread(&PGClient::BackgroundThread, this, std::move(signal));
	
	return true;
}

bool PGClient::InitZMQ(){
	
	/*                  ZMQ Setup                */
	/* ----------------------------------------- */
	
	// we have two zmq sockets:
	// 1. [PUB]    one for sending write queries to all listeners (the master)
	// 2. [DEALER] one for sending read queries round-robin and receving responses
	
	// specify the ports everything talks/listens on
	clt_pub_port = 77778;   // for sending write queries
	clt_dlr_port = 77777;   // for sending read queries
	// socket timeouts, so nothing blocks indefinitely
	int clt_pub_socket_timeout=500;
	int clt_dlr_socket_timeout=500;  // both send and receive
	
	// poll timeouts - units are milliseconds
	inpoll_timeout=500;
	outpoll_timeout=500;
	
	// total timeout on how long we wait for response from a query
	query_timeout=2000;
	
	// Update with user-specified values.
	m_variables.Get("clt_pub_port",clt_pub_port);
	m_variables.Get("clt_dlr_port",clt_dlr_port);
	m_variables.Get("clt_pub_socket_timeout",clt_pub_socket_timeout);
	m_variables.Get("clt_dlr_socket_timeout",clt_dlr_socket_timeout);
	m_variables.Get("inpoll_timeout",inpoll_timeout);
	m_variables.Get("outpoll_timeout",outpoll_timeout);
	m_variables.Get("query_timeout",query_timeout);
	
	// to send replies the middleman must know who to send them to.
	// for read queries, the receiving router socket will append the ZMQ_IDENTITY of the sender
	// which can be given to the sending router socket to identify the recipient.
	// BUT the default ZMQ_IDENTITY of a socket is empty! We must set it ourselves to be useful!
	// for write queries we ALSO need to manually insert the ZMQ_IDENTITY into the written message,
	// because the receiving sub socket does not do this automaticaly.
	
	// using 'getsockopt(ZMQ_IDENTITY)' without setting it first produces an empty string,
	// so seems to need to set it manually to be able to know what the ID is, and
	// insert it into the write queries.
	// FIXME replace with whatever ben wants?
	boost::uuids::uuid u = boost::uuids::random_generator()();
	clt_ID = boost::uuids::to_string(u);
	clt_ID += '\0';
	
	// get zmq context from datamodel, or make one if none
	if(m_data!=nullptr && m_data->context!=nullptr){
		context = m_data->context;
	} else {
		int context_io_threads = 1;
		context = new zmq::context_t(context_io_threads);
	}
	
	// socket to publish write queries
	// -------------------------------
	clt_pub_socket = new zmq::socket_t(*context, ZMQ_PUB);
	clt_pub_socket->setsockopt(ZMQ_SNDTIMEO, clt_pub_socket_timeout);
	clt_pub_socket->bind(std::string("tcp://*:")+std::to_string(clt_pub_port));
	
	// socket to deal read queries and receive responses
	// -------------------------------------------------
	clt_dlr_socket = new zmq::socket_t(*context, ZMQ_DEALER);
	clt_dlr_socket->setsockopt(ZMQ_SNDTIMEO, clt_dlr_socket_timeout);
	clt_dlr_socket->setsockopt(ZMQ_RCVTIMEO, clt_dlr_socket_timeout);
	clt_dlr_socket->setsockopt(ZMQ_IDENTITY, clt_ID.c_str(), clt_ID.length());
	clt_dlr_socket->bind(std::string("tcp://*:")+std::to_string(clt_dlr_port));
	
	// bundle the polls together so we can do all of them at once
	zmq::pollitem_t clt_pub_socket_pollout= zmq::pollitem_t{*clt_pub_socket,0,ZMQ_POLLOUT,0};
	zmq::pollitem_t clt_dlr_socket_pollin = zmq::pollitem_t{*clt_dlr_socket,0,ZMQ_POLLIN,0};
	zmq::pollitem_t clt_dlr_socket_pollout = zmq::pollitem_t{*clt_dlr_socket,0,ZMQ_POLLOUT,0};
	
	in_polls = std::vector<zmq::pollitem_t>{clt_dlr_socket_pollin};
	out_polls = std::vector<zmq::pollitem_t>{clt_pub_socket_pollout,
	                                         clt_dlr_socket_pollout};
	
	return true;
}

bool PGClient::InitServiceDiscovery(){
	
	// this is tricky because we can't access it from the DataModel to know
	// if the toolchain is already running a servicedicovery thread or not.
	// a crude check
	std::string sda;
	if(m_data!=nullptr && m_data->vars.Get("service_discovery_address",sda)==true){
		// probably running as part of a toolchain
		Log("Seem to be part of a toolchain; assuming ServiceDiscovery is running",v_message,verbosity);
		return true;
	}
	// otherwise assume we have to start one of our own
	Log("Creating ServiceDiscovery thread",v_message,verbosity);
	
	/*               Service Discovery           */
	/* ----------------------------------------- */
	
	// Use a service discovery class to broadcast messages signalling our presence.
	// The middlemen can pick up on these broadcasts and connect to us to receive data.
	// Read service discovery configs into a Store
	std::string service_discovery_config;
	m_variables.Get("service_discovery_config", service_discovery_config);
	Store service_discovery_configstore;
	service_discovery_configstore.Initialise(service_discovery_config);
	
	// we do wish to send broadcasts advertising our services
	bool send_broadcasts = true;
	// the ServiceDiscovery class can also listen for other service providers, but we don't need to.
	bool rcv_broadcasts = false;
	
	// multicast address and port to broadcast on. must match the middleman.
	std::string broadcast_address = "239.192.1.1";
	int broadcast_port = 5000;
	service_discovery_configstore.Set("broadcast_address",broadcast_address);
	service_discovery_configstore.Set("broadcast_port",broadcast_port);
	
	// how frequently to broadcast
	int broadcast_period_sec = 5;
	service_discovery_configstore.Set("broadcast_period",broadcast_period_sec);
	
	// a unique identifier for us. this is used by the ServiceDiscovery listener thread,
	// which maintains a map of services it's recently heard about, for which this is the key.
	boost::uuids::uuid client_id = boost::uuids::random_generator()();
	
	// a service name. The Utilities class then has a helper function that will connect
	// a given zmq socket to all broadcasters with a given service name, which allows
	// the middlemen to connect a receive_read_query socket to all discovered clients'
	// send_read_query sockets, for example. In the constructor the service name is the
	// name of the service used for RemoteControl (which we will not be using).
	std::string client_name = "DemoClient";
	service_discovery_configstore.Get("client_name",client_name);
	
	// the ServiceDiscovery class will inherently advertise the presence of a remote control port,
	// and will attempt to check the status of that service on each broadcast by querying
	// localhost:remote_control_port. Unless we implement a listener to respond to those messages,
	// the zmq poll will timeout and the reported Status will just be "Status".
	// (the RemoteControl Service is normally implemented as part of ToolDAQ)
	int remote_control_port = 24011;
	
	// construct the ServiceDiscovery instance.
	// it'll handle the broadcasting in a separate thread - we don't need to do anything else.
	service_discovery = new ServiceDiscovery(send_broadcasts, rcv_broadcasts, remote_control_port,
	                                         broadcast_address, broadcast_port, context, client_id,
	                                         client_name, broadcast_period_sec);
	
	return true;
}

bool PGClient::RegisterServices(){
	
	/*             Register Services             */
	/* ----------------------------------------- */
	
	// to register our query and response ports with the ServiceDiscovery class
	// we can make our lives a little easier by using a Utilities class
	utilities = new DAQUtilities(context);
	
	// we can now register the client sockets with the following:
	utilities->AddService("psql_write", clt_pub_port);
	utilities->AddService("psql_read",  clt_dlr_port);
	
	return true;
}

bool PGClient::InitLogging(){
	
	// get logger class from datamodel, or make if there isn't one
	if(m_data!=nullptr && m_data->Log!=nullptr){
		m_log = m_data->Log;
	} else {
		bool log_interactive = true;
		bool log_local = false;
		std::string log_local_path = "";
		bool log_split_files = false;
		m_log = new Logging(log_interactive, log_local, log_local_path, log_split_files);
	}
	
	return true;
}

void PGClient::Log(std::string msg, int msg_verb, int verbosity){
	// this is normally defined in Tool.h
	m_log->Log(msg, msg_verb, verbosity);
}

bool PGClient::TestMe(){
	

	
	return true;
}

bool PGClient::BackgroundThread(std::future<void> signaller){
	
	std::cout<<"BackgroundThread starting!"<<std::endl;
	while(true){
		// check if we've been signalled to terminate
		std::chrono::milliseconds span(10);
		if(signaller.wait_for(span)!=std::future_status::timeout){
			// terminate has been set
			std::cout<<"background thread received terminate signal"<<std::endl;
			break;
		}
		
		// otherwise continue our duties
		get_ok = GetNextRespose();
		get_ok = SendNextQuery();
		//get_ok = FindNewClients();     FOR MIDDLEMAN ONLY
	}
	
	return true;
}

bool PGClient::SendQuery(std::string dbname, std::string query_string, std::vector<std::string>* results, int* timeout_ms, std::string* err){
	// send a query and receive response.
	// This is a wrapper that ensures we always return within the requested timeout.
	
	// we need to send reads and writes to different sockets.
	// we could ask the user to specify, or try to determine it ourselves
	bool is_write_txn = (query_string.find("INSERT")!=std::string::npos) ||
	                    (query_string.find("UPDATE")!=std::string::npos) ||
	                    (query_string.find("DELETE")!=std::string::npos);
	char type = (is_write_txn) ? 'w' : 'r';
	
	// encapsulate the query in an object.
	// We need this since we can only get one return value from an asynchronous function call,
	// and we want both a response string and error flag.
	Query qry{dbname, query_string, type};
	
	// submit the query asynchrously.
	// This way we have control over how long we wait for the response
	// The response will be a Query object with remaining members populated.
	std::future<Query> response = std::async(std::launch::async, &PGClient::DoQuery, this, qry);
	
	// the return from a std::async call is a 'future' object
	// this object will be populated with the return value when it becomes available,
	// but we can wait for a given timeout and then bail if it hasn't resolved in time.
	
	int timeout=query_timeout;              // default timeout for submission of query and receipt of response
	if(timeout_ms) timeout=*timeout_ms;     // override by user if a custom timeout is given
	std::chrono::milliseconds span(timeout);
	// wait_for will return either when the result is ready, or when it times out
	if(response.wait_for(span)!=std::future_status::timeout){
		// we got a response in time. retrieve and parse return value
		qry = response.get();
		if(results) *results = qry.query_response;
		if(err) *err = qry.err;
		return qry.success;
	} else {
		// timed out
		std::string errmsg="Timed out after waiting "+std::to_string(timeout)+"ms for response "
		                   "from read query '"+query_string+"'";
		if(verbosity>3) std::cerr<<errmsg<<std::endl;
		if(err) *err=errmsg;
		return false;
	}
	
	return false;  // dummy
}

bool PGClient::SendQuery(std::string dbname, std::string query_string, std::string* results, int* timeout_ms, std::string* err){
	// wrapper for when user expects only one returned row
	if(err) *err="";
	std::vector<std::string> resultsvec;
	bool ret = SendQuery(dbname, query_string, &resultsvec, timeout_ms, err);
	if(resultsvec.size()>0 && results!=nullptr) *results = resultsvec.front();
	// if more than one row returned, flag as error
	if(resultsvec.size()>1){
		*err += ". Query returned "+std::to_string(resultsvec.size())+" rows!";
		ret=false;
	}
	return ret;
}

Query PGClient::DoQuery(Query qry){
	std::cout<<"PGClient DoQuery received query"<<std::endl;
	// submit a query, wait for the response and return it
	
	// capture a unique id for this message
	int thismsgid = ++msg_id;
	qry.msg_id = thismsgid;
	
	// zmq sockets aren't thread-safe, so we have one central sender.
	// submit our query and keep a ticket to retrieve the return status on completion
	std::promise<int> send_ticket;
	std::future<int> send_receipt = send_ticket.get_future();
	std::cout<<"PGClient enqueing query "<<qry.msg_id<<std::endl;
	waiting_senders.emplace(qry, std::move(send_ticket));
	
	// wait for our number to come up. loooong timeout, but don't hang forever.
	if(send_receipt.wait_for(std::chrono::seconds(30))==std::future_status::timeout){
		// sending timed out
		if(qry.type=='w') ++write_queries_failed;
		else if(qry.type=='r') ++read_queries_failed;
		Log("Timed out sending query "+std::to_string(thismsgid),v_warning,verbosity);
		qry.success = false;
		qry.err = "Timed out sending query";
		return qry;
	} // else got a return value
	
	// check for errors sending
	int ret = send_receipt.get();
	std::string errmsg;
	if(ret==-3) errmsg="Error polling out socket in PollAndSend! Is socket closed?";
	if(ret==-2) errmsg="No listener on out socket in PollAndSend!";
	if(ret==-1) errmsg="Error sending in PollAndSend!";
	if(ret!=0){
		if(qry.type=='w') ++write_queries_failed;
		else if(qry.type=='r') ++read_queries_failed;
		Log(errmsg,v_debug,verbosity);
		qry.success = false;
		qry.err = errmsg;
		return qry;
	}
	
	// if we succeeded in sending the message, we now need to wait for a repsonse.
	// again, the next message received may not be for us, so a central dealer receives
	// all responses and deals them out to the appropriate recipient.
	// submit a ticket for our message id and wait for it to come up.
	std::promise<Query> response_ticket;
	std::future<Query> response_reciept = response_ticket.get_future();
	waiting_recipients.emplace(thismsgid, std::move(response_ticket));
	
	// wait for our number to come up. loooong timeout, but don't hang forever.
	if(response_reciept.wait_for(std::chrono::seconds(30))==std::future_status::timeout){
		// timed out
		if(qry.type=='w') ++write_queries_failed;
		else if(qry.type=='r') ++read_queries_failed;
		Log("Timed out waiting for response for query "+std::to_string(thismsgid),v_warning,verbosity);
		qry.success = false;
		qry.err = "Timed out waiting for response";
		return qry;
	} else {
		std::cout<<"PGClient got a response for query "<<qry.msg_id<<std::endl;
		// got a response!
		return response_reciept.get();
	}
	
	return qry;  // dummy
	
}

bool PGClient::GetNextRespose(){
	// get any new messages from middleman, and notify the client of the outcome
	
	std::vector<zmq::message_t> response;
	int ret = PollAndReceive(clt_dlr_socket, in_polls.at(0), inpoll_timeout, response);
	//std::cout<<"PGClient: GNR returned "<<ret<<std::endl;
	
	// check return status
	if(ret==-2) return true;      // no messages waiting to be received
	
	if(ret==-3){
		Log("PollAndReceive Error polling in socket! Is socket closed?",v_error,verbosity);
		return false;
	}
	
	if(response.size()==0){
		Log("PollAndReceive recieved empty response!",v_error,verbosity);
		return false;
	}
	
	// received message may be an acknowledgement of a write, or the result of a read.
	// messages are 2+ zmq parts as follows:
	// 1. the message ID, used by the client to match to the message it sent
	// 2. the response code, to signal errors
	// 3.... the SQL query results, if any. Each row is returned in a new message part.
	Query qry;
	if(ret==-1 || response.size()<2){
		// return of -1 suggests the last zmq message had the 'more' flag set
		// suggesting there should have been more parts, but they never came.
		qry.success = false;
		qry.err="Received incomplete zmq response";
		Log(qry.err,v_warning,verbosity);
		if(ret==-1) Log("Last message had zmq more flag set",v_warning,verbosity);
		if(response.size()<2) Log("Only received "+std::to_string(response.size())+" parts",v_warning,verbosity);
		// continue to parse as much as we can - the first part identifies the query,
		// so we can at least inform the client of the failure
	}
	// else if ret==0 && response.size() >= 2: success
	
	// if we got this far we had at least one response part; the message id
	int message_id_rcvd = *reinterpret_cast<int*>(response.at(0).data());
	
	// if we also had a status part, get that
	if(response.size()>1){
		qry.success = *reinterpret_cast<int*>(response.at(1).data());  // (0 or 1 for now)
	}
	// if we also had further parts, fetch those
	for(int i=2; i<response.size(); ++i){
		qry.query_response.push_back(std::string(reinterpret_cast<const char*>(response.at(i).data())));
	}
	
	// get the ticket associated with this message id
	if(waiting_recipients.count(message_id_rcvd)){
		std::promise<Query>* ticket = &waiting_recipients.at(message_id_rcvd);
		ticket->set_value(qry);
		// remove it from the map of waiting promises
		waiting_recipients.erase(message_id_rcvd);
	} else {
		// unknown message id?
		Log("Unknown message id "+std::to_string(message_id_rcvd)+" with no client",v_error,verbosity);
		return false;
	}
	
	return true;
}

bool PGClient::SendNextQuery(){
	// send the next message in the waiting query queue
	
	if(waiting_senders.empty()){
		// nothing to send
		return true;
	}
	
	// get the next query to send
	std::pair<Query, std::promise<int>>& next_qry = waiting_senders.front();
	Query qry = next_qry.first;
	std::cout<<"PGClient: sending query "<<qry.msg_id<<std::endl;
	
	// write queries go to the pub socket, read queries to the dealer
	zmq::socket_t* thesocket = (qry.type=='w') ? clt_pub_socket : clt_dlr_socket;
	
	// send out the query
	// queries should be formatted as 4 parts:
	// 1. client ID     (automatically prepended by our dealer socket)
	// 2. message ID
	// 3. database name
	// 4. SQL statement
	int ret = PollAndSend(thesocket, out_polls.at(1), outpoll_timeout, qry.msg_id, qry.dbname, qry.query_string);
	std::cout<<"PGClient SNQ P&S returned "<<ret<<std::endl;
	
	// notify the client that the message has been sent
	std::promise<int>* ticket = &next_qry.second;
	ticket->set_value(ret);
	
	// pop the message off the queue; it has been sent.
	waiting_senders.pop();
	
	return true;
	
}

bool PGClient::Finalise(){
	// terminate our background thread
	std::cout<<"sending background thread term signal"<<std::endl;
	terminator.set_value();
	// wait for it to finish up and return
	std::cout<<"waiting for background thread to rejoin"<<std::endl;
	background_thread.join();
	
	std::cout<<"Removing services"<<std::endl;
	utilities->RemoveService("psql_write");
	utilities->RemoveService("psql_read");
	
	std::cout<<"Deleting ServiceDiscovery"<<std::endl;
	delete service_discovery; service_discovery=nullptr;
	
	std::cout<<"Deleting Utilities class"<<std::endl;
	delete utilities; utilities=nullptr;
	
	std::cout<<"deleting sockets"<<std::endl;
	delete clt_pub_socket; clt_pub_socket=nullptr; 
	delete clt_dlr_socket; clt_dlr_socket=nullptr;
	
	std::cout<<"deleting context"<<std::endl;
	// only delete context if it's local, not from the parent DataModel.
	if(m_data==nullptr || m_data->context==nullptr) delete context; context=nullptr;
	
	// same with logging
	if(m_data==nullptr || m_data->Log==nullptr) delete m_log; m_log=nullptr;
	
	// can't use 'Log' since we may have deleted the Logging class
	std::cout<<"PGClient destructor done"<<std::endl;
}

// =====================================================================
// function adapter from same in middleman ReceiveSQL
bool PGClient::FindNewClients(){
	
	int old_conns=connections.size();
	
	// update any connections
	utilities->UpdateConnections("psql_write", clt_pub_socket, connections);
	utilities->UpdateConnections("psql_read", clt_dlr_socket, connections);
	
	if(old_conns!=connections.size()){
		Log("Made "+std::to_string(connections.size()-old_conns)+" new connections!",v_warning,verbosity);
	} else {
		Log("No new clients found",v_debug,verbosity);
	}
	/*
	std::cout<<"We have: "<<connections.size()<<" connected clients"<<std::endl;
	std::cout<<"Connections are: "<<std::endl;
	for(auto&& athing : connections){
		std::string service;
		athing.second->Get("msg_value",service);
		std::cout<<service<<" connected on "<<athing.first<<std::endl;
	}
	*/
	
	return true;
}

// =====================================================================
// ZMQ helper functions; TODO move these to external class? since they're shared by middleman.

bool PGClient::Send(zmq::socket_t* sock, bool more, zmq::message_t& message){
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	return send_ok;
}

bool PGClient::Send(zmq::socket_t* sock, bool more, std::string messagedata){
	// form the zmq::message_t
	zmq::message_t message(messagedata.size()+1);
	snprintf((char*)message.data(), messagedata.size()+1, "%s", messagedata.c_str());
	
	// send it with given SNDMORE flag
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	
	return send_ok;
}

bool PGClient::Send(zmq::socket_t* sock, bool more, std::vector<std::string> messages){
	
	// loop over all but the last part in the input vector,
	// and send with the SNDMORE flag
	for(int i=0; i<(messages.size()-1); ++i){
		
		// form zmq::message_t
		zmq::message_t message(messages.at(i).size()+1);
		snprintf((char*)message.data(), messages.at(i).size()+1, "%s", messages.at(i).c_str());
		
		// send this part
		bool send_ok = sock->send(message, ZMQ_SNDMORE);
		
		// break on error
		if(not send_ok) return false;
	}
	
	// form the zmq::message_t for the last part
	zmq::message_t message(messages.back().size()+1);
	snprintf((char*)message.data(), messages.back().size()+1, "%s", messages.back().c_str());
	
	// send it with, or without SNDMORE flag as requested
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	
	return send_ok;
}

int PGClient::PollAndReceive(zmq::socket_t* sock, zmq::pollitem_t poll, int timeout, std::vector<zmq::message_t>& outputs){
	
	// poll the input socket for messages
	get_ok = zmq::poll(&poll, 1, timeout);
	if(get_ok<0){
		// error polling - is the socket closed?
		return -3;
	}
	
	// check for messages waiting to be read
	if(poll.revents & ZMQ_POLLIN){
		
		// recieve all parts
		get_ok = Receive(sock, outputs);
		if(not get_ok) return -1;
		
	} else {
		// no waiting messages
		return -2;
	}
	// else received ok
	return 0;
}

bool PGClient::Receive(zmq::socket_t* sock, std::vector<zmq::message_t>& outputs){
	
	outputs.clear();
	int part=0;
	
	// recieve parts into tmp variable
	zmq::message_t tmp;
	while(sock->recv(&tmp)){
		
		// transfer the received message to the output vector
		outputs.resize(outputs.size()+1);
		outputs.back().move(&tmp);
		
		// receive next part if there is more to come
		if(!outputs.back().more()) break;
		
	}
	
	// if we broke the loop but last successfully received message had a more flag,
	// we must have broken due to a failed receive
	if(outputs.back().more()){
		// sock->recv failed
		return false;
	}
	
	// otherwise no more parts. done.
	return true;
}

