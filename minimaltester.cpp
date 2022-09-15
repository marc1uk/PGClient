#include "PGHelper.h"
#include "Store.h"
#include "DataModel.h"

int main(int argc, const char** argv){
	
	if(argc<2){
		std::cout<<"usage: "<<argv[0]<<" <configfile>"<<std::endl;
		return 0;
	}
	
	Store configfile;
	configfile.Initialise(argv[1]);
	
	std::string stop_file;
	bool get_ok = configfile.Get("stopfile",stop_file);
	if(not get_ok){
		std::cout<<"Please include 'stopfile' in configuration"<<std::endl;
		std::cout<<"Program will terminate when the stopfile is found"<<std::endl;
		return false;
	}
	std::ifstream test(stop_file.c_str());
	if(test.is_open()){
		std::string cmd = "rm "+stop_file;
		system(cmd.c_str());
	}
	
	PGClient theclient;
	get_ok = theclient.Initialise(argv[1]);
	if(not get_ok) return false;
	
	int loopi=0;
	while(true){
		
		++loopi;
		
		// run a read query
		std::vector<std::string> results;
		std::string err;
		std::string dbname="rundb";
		std::string query_string = "SELECT max(runnum) FROM run";
		int this_timeout = 100;
		std::cout<<"submitting read query"<<std::endl;
		get_ok = theclient.SendQuery(dbname, query_string, &results, &this_timeout, &err);
		std::cout<<"read query "<<loopi<<" returned "<<get_ok<<", err='"<<err<<"'"<<", results='";
		for(int i=0; i<results.size(); ++i){
			if(i>0) std::cout<<", ";
			std::cout<<results.at(i);
		}
		std::cout<<"'"<<std::endl;
		
		// run a write query
		dbname="monitoringdb";
		query_string = "INSERT INTO logging ( time, source, severity, message ) VALUES ( 'now()', 'debug', 99, 'testing pgclient " + std::to_string(loopi)+"' );";
		std::cout<<"doing write query"<<std::endl;
		results.clear();
		get_ok = theclient.SendQuery(dbname, query_string, &results, &this_timeout, &err);
		std::cout<<"write query "<<loopi<<" returned "<<get_ok<<", err='"<<err<<"'"<<", results='";
		for(int i=0; i<results.size(); ++i){
			if(i>0) std::cout<<", ";
			std::cout<<results.at(i);
		}
		std::cout<<"'"<<std::endl;
		
		// check for stop file
		std::ifstream stopfile(stop_file);
		if (stopfile.is_open()){
			std::cout<<"Stopfile found, terminating"<<std::endl;
			stopfile.close();
			std::string cmd = "rm "+stop_file;
			system(cmd.c_str());
			break;
		}
		
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	
	theclient.Finalise();
	
	return 0;
}
