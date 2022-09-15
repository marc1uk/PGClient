#include "PGHelper.h"
#include "DataModel.h"
#include <sstream>
#include <exception>

// TODO rewrite these using local query wrappers and no dependence on pqxx

PGHelper::PGHelper(DataModel* m_data_in) : m_data(m_data_in) {}

void PGHelper::SetDataModel(DataModel* m_data_in){
	m_data = m_data_in;
}

void PGHelper::SetVerbosity(int verb){
	verbosity=verb;
}

bool PGHelper::GetCurrentRun(int& runnum, int* runconfig, std::string* err){
//	if(verbosity>v_debug) std::cout<<"Getting current run"<<std::endl;
//	
//	// query the latest run number from the run database
//	std::string query_string = "SELECT max(runnum) FROM run;";
//	if(verbosity>v_debug) std::cout<<"Querying max run number"<<std::endl;
//	get_ok = m_data->postgres.ExecuteQuery(query_string, runnum);
//	if(not get_ok){
//		std::string errmsg="failed to get max runnum from run table";
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	if(verbosity>v_debug) std::cout<<"Max run number is "<<runnum<<std::endl;
//	
//	// if we're given somewhere to put the runconfig, get that as well while we're at it.
//	if(runconfig){
//		query_string = "SELECT runconfig FROM run WHERE runnum = "+std::to_string(runnum);
//		if(verbosity>v_debug) std::cout<<"Getting runconfig with query: \n"<<query_string<<"\n";
//		get_ok = m_data->postgres.ExecuteQuery(query_string, *runconfig);
//		if(not get_ok){
//			std::string errmsg="failed to get runconfig for run "+std::to_string(runnum);
//			std::cerr<<errmsg<<std::endl;
//			if(err) *err = errmsg;
//			return false;
//		}
//		if(verbosity>v_debug) std::cout<<"run config ID: "<<runconfig<<std::endl;
//	}
//	
//	// return success
	return true;
}

bool PGHelper::GetRunConfig(int& runconfig, int* runnum_in, std::string* err){
//	// Get the run config ID for a given run number. If no run number is given,
//	// the run config ID in m_data->vars will be used.
//	
//	// first we need a run number. see if we're given one
//	int runnum=-1;
//	if(runnum_in){
//		runnum=*runnum_in;
//		if(runnum<0){
//			std::string errmsg="GetRunConfig called with negative run number";
//			std::cerr<<errmsg<<std::endl;
//			if(err) *err=errmsg;
//			return false;
//		}
//	} else {
//		// not given one.
//		if(verbosity>v_debug) std::cout<<"no run number; getting runconfig from vars"<<std::endl;
//		
//		// Assume the latest run in the rundb
//		if(verbosity>v_debug) std::cout<<"No run number given, looking up latest run"<<std::endl;
//		get_ok = GetCurrentRun(runnum, &runconfig, err);
//		// this gets the runconfig at the same time, and will populate any error, so we're done.
//		
//		return get_ok;
//	}
//	if(verbosity>v_debug) std::cout<<"getting runconfig for run "<<runnum<<std::endl;
//	
//	// else given a run number to use; query db for corresponding runconfig.
//	std::string query_string = "SELECT runconfig FROM run WHERE runnum = "+ std::to_string(runnum)+";";
//	get_ok = m_data->postgres.ExecuteQuery(query_string, runconfig);
//	if(not get_ok){
//		std::string errmsg="failed to get runconfig for run "+std::to_string(runnum);
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	if(verbosity>v_debug) std::cout<<"run config ID: "<<runconfig<<std::endl;
//	
	return true;
}

bool PGHelper::GetSystemConfig(int& systemconfig, std::string* systemname_in, int* runnum_in, int* runconfig_in, std::string* err){
//	// get the ID of the system configuration for a given runconfig.
//	// if system name is given, use it, otherwise use the system name in m_data->vars.
//	// if runconfig is given, look up system config for that run configuration.
//	// else if run number is given, look up runconfig for that run and use it.
//	// else get current (maximum) run number and use that.
//	
//	// get system name
//	std::string systemnametmp="";
//	std::string* systemnamep=&systemnametmp;
//	if(systemname_in!=nullptr){
//		systemnamep=systemname_in;
//	}
//	if(*systemnamep==""){
//		get_ok = m_data->vars.Get("SystemName",*systemnamep);
//		if(!get_ok){
//			std::string errmsg="GetSystemConfig called with no system name and none in m_data->vars";
//			std::cerr<<errmsg<<std::endl;
//			if(err) *err = errmsg;
//			return false;
//		}
//	}
//	
//	// get run configuration
//	int runconfig=-1;
//	if(runconfig_in!=nullptr){
//		runconfig=*runconfig_in;
//		if(runconfig<0){
//			std::string errmsg="GetSystemConfig called with negative runconfig "+std::to_string(runconfig);
//			std::cerr<<errmsg<<std::endl;
//			if(err) *err=errmsg;
//			return false;
//		}
//	} else {
//		// no runconfig given, look it up
//		get_ok = GetRunConfig(runconfig, runnum_in, err);
//		if(!get_ok) return false;
//	}
//	
//	// define the query to retreive the system config ID
//	std::string query_string = "SELECT "+m_data->postgres.pqxx_quote_name(*systemnamep)
//	                           +" FROM runconfig WHERE id = "
//	                           +m_data->postgres.pqxx_quote(runconfig)+" ;";
//	
//	// perform the query
//	if(verbosity>v_debug){
//		std::cout<<"Getting configuration for system "<<*systemnamep<<", runconfig "<<runconfig
//		          <<" with query \n"<<query_string<<"\n";
//	}
//	get_ok = m_data->postgres.ExecuteQuery(query_string, systemconfig);
//	
//	if(not get_ok){
//		std::string errmsg="error getting config for system "+*systemnamep
//		                  +", runconfig "+std::to_string(runconfig);
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	
	return true;
}

bool PGHelper::GetToolsConfig(std::string& toolsconfig, std::string* systemname_in, int* runnum_in, int* runconfig_in, int* systemconfig_in, std::string* err){
//	// get the text representing list of tools and their configfile version nums
//	// if runconfig is given, toolsconfig for that runconfig will be used
//	// else if runnum is given, toolsconfig for that runconfig will be used.
//	// if neither are given the toolsconfig for the runconfig number in m_data->vars will be used
//	
//	// get system config
//	int systemconfig;
//	std::string systemname="";
//	if(systemname_in) systemname=*systemname_in;
//	if(systemconfig_in){
//		systemconfig=*systemconfig_in;
//	} else {
//		get_ok = GetSystemConfig(systemconfig, &systemname, runnum_in, runconfig_in, err);
//		if(!get_ok) return false;
//	}
//	if(systemname_in) *systemname_in = systemname;
//	
//	// build the query
//	std::string query_string = "SELECT toolsconfig FROM "+m_data->postgres.pqxx_quote_name(systemname)+" WHERE id = "
//	                           +pqxx::to_string(systemconfig)+" ;";
//	// perform the query
//	if(verbosity>v_debug){
//		std::cout<<"Getting list of tools and configfile versions with query \n"<<query_string<<"\n";
//	}
//	get_ok = m_data->postgres.ExecuteQuery(query_string, toolsconfig);
//	// FIXME replace ExecuteQuery call with Query to get better error info?
//	// or perhaps better, add error info to Query method?
//	if(not get_ok){
//		std::string errmsg="error getting toolsconfig for system "+systemname+", id "+std::to_string(systemconfig);
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	
	return true;
}

bool PGHelper::GetToolConfig(std::string toolname, std::string& toolconfig, std::string* systemname_in, int* runnum_in, int* runconfig_in, int* systemconfig_in, std::string* err){
//	// Get the tool configuration for the given tool in the given toolchain
//	
//	// first get the toolsconfig for the current run config
//	std::string toolsconfig;
//	std::string systemname="";
//	if(systemname_in) systemname=*systemname_in;
//	if(verbosity>v_debug) std::cout<<"getting toolsconfig"<<std::endl;
//	get_ok = GetToolsConfig(toolsconfig, &systemname, runnum_in, runconfig_in, systemconfig_in, err);
//	if(not get_ok) return false;
//	
//	// parse it into a store
//	if(verbosity>v_debug) std::cout<<"parsing toolsconfig"<<std::endl;
//	Store toolsconfigstore;
//	//get_ok = ParseToolsConfig(toolsconfig, toolsconfigstore, err);  // for parsing plain strings
//	//if(not get_ok) return false;
//	toolsconfigstore.JsonParser(toolsconfig);  // for parsing json maps of toolname to version num
//	
//	// extract the entry corresponding to the given unique identifier of this Tool instance.
//	double versionnum=-1;
//	if(verbosity>v_debug) std::cout<<"getting config version for tool instance "<<toolname<<std::endl;
//	get_ok = toolsconfigstore.Get(toolname, versionnum);
//	if(not get_ok || versionnum<0){
//		std::string errmsg = std::string("PGHelper::GetToolConfig did not find tool '")
//		                     +toolname+"' in the toolsconfig for the current runconfig!";
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err=errmsg;
//		return false;
//	}
//	
//	// use the class name, system name and configfile version to get the tool config
//	if(verbosity>v_debug) std::cout<<"querying configuration for tool "<<toolname
//	                               <<", version "<<versionnum<<std::endl;
//	get_ok = GetToolConfig(toolconfig, systemname, toolname, versionnum, err);
//	
	return get_ok;
}

bool PGHelper::GetToolConfig(std::string& toolconfig, std::string systemname, std::string className, double version, std::string* err){
//	// use the tool class name and version number to look up the Tool config
//	
//	std::string query_string = "SELECT contents FROM configfiles WHERE tool = "
//	                           +m_data->postgres.pqxx_quote(className)
//	                           +" AND system = "+m_data->postgres.pqxx_quote(systemname)
//	                           +" AND version = "+std::to_string(version)+" ;";
//	
//	// perform the query
//	if(verbosity>v_debug){
//		std::cout<<"Getting configuration for tool "<<className<<" in system "<<systemname
//		         <<", version "<<version<<" with query \n"<<query_string<<"\n";
//	}
//	get_ok = m_data->postgres.ExecuteQuery(query_string, toolconfig);
//	
//	if(not get_ok){
//		std::string errmsg="error getting config for tool "+className
//		                  +", version "+std::to_string(version);
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	
	return true;
}

int PGHelper::InsertToolConfig(Store config, std::string toolname, std::string author, std::string description, std::string* systemname_in, std::string* err){
//	// insert a new Tool configuration entry
//	
//	// each entry in the configfiles table contains:
//	// a tool name,          <-| together these 3 uniquely
//	// a version number,     <-| identify a config file entry
//	// a system name,        <-| 
//	// an author,
//	// a creation timestamp,
//	// a description,
//	// the config file contents
//	
//	// get system name
//	std::string systemname;
//	if(systemname_in){
//		systemname=*systemname_in;
//	} else {
//		get_ok = m_data->vars.Get("SystemName",systemname);
//		if(!get_ok){
//			std::string errmsg = "InsertToolConfig given no system name and found none in m_data->vars";
//			std::cerr<<errmsg<<std::endl;
//			if(err) *err=errmsg;
//			return false;
//		}
//	}
//	
//	// Use the Store streamer to generate the json string
//	std::string json_string;
//	config >> json_string;
//	
//	// we'll allocate a new version number as a sequence of config files for this tool
//	// we do this manually, for now. get the current max version number for this tool.
//	std::string query_string = "SELECT max(version) FROM configfiles WHERE name = "
//	                         + m_data->postgres.pqxx_quote(toolname) + "AND system = "
//	                         + m_data->postgres.pqxx_quote(systemname);
//	if(verbosity>v_debug){
//		std::cout<<"Querying max version number for tool "<<toolname<<std::endl;
//	}
//	pqxx::row returnedrow;
//	get_ok = m_data->postgres.Query(query_string, 2, nullptr, &returnedrow, err);
//	if(not get_ok){
//		std::string errmsg="error getting max version number for tool "+toolname;
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err = errmsg;
//		return false;
//	}
//	// else extract the result from the returned row:
//	int versionnum = returnedrow[0].as<int>();
//	++versionnum; // our new entry will be the next one up.
//	if(verbosity>v_debug) std::cout<<"This config file will be config version number "<<versionnum<<std::endl;
//	
//	// creation timestamp will also be automatically generated as NOW()
//	std::string created="NOW()";
//	
//	std::vector<std::string> fields_to_fill{"system", "tool", "version", "author", "created", "description", "contents"};
//	get_ok = m_data->postgres.Insert("configfiles", fields_to_fill, err, systemname,
//	                                                                     toolname,
//	                                                                     versionnum,
//	                                                                     author,
//	                                                                     created,
//	                                                                     description,
//	                                                                     json_string
//	                                                                     );
//	
	return get_ok; // return if we succeeded - if not error will already be populated
}

bool PGHelper::ParseToolsConfig(std::string toolsconfig, BStore& configStore, std::string* err){
//	std::string errmsg="errors occurred converting config version to double for tools: ";
//	std::string line;
//	std::string uniqueName, className, versionNum;
//	std::stringstream toolsconfigstream(toolsconfig);
//	bool get_ok = true;
//	while(getline(toolsconfigstream,line)){
//		if(line[0]=='#') continue;
//		if(line.empty()) continue;
//		std::stringstream ss(line);
//		ss >> uniqueName >> className >> versionNum;
//		//std::cout<<uniqueName<<":"<<className<<":"<<versionNum<<std::endl;
//		
//		// some tools may not need any configuration variables,
//		// ben has a habit of using 'NULL' in such cases
//		if(versionNum=="NULL") versionNum="-1";
//		
//		// convert the version number to a double
//		try {
//			//std::cout<<"converting "<<versionNum<<" to double"<<std::endl;
//			double version = std::stod(versionNum);
//			//std::cout<<"version="<<version<<std::endl;
//			configStore.Set(uniqueName, std::pair<std::string,double>{className, version});
//		} catch (...){
//			get_ok = false;
//			errmsg += "'"+uniqueName+"'";
//		}
//	}
//	if(!get_ok){
//		std::cerr<<errmsg<<std::endl;
//		if(err) *err=errmsg;
//	}
	return get_ok;
}

