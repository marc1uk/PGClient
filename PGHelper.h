#ifndef PGHELPER_H
#define PGHELPER_H

#include "Store.h"
#include "BStore.h"

class DataModel;

class PGHelper{
	public:
	PGHelper(DataModel* m_data_in=nullptr);
	void SetDataModel(DataModel* m_data_in);
	void SetVerbosity(int verb);
	
	bool GetCurrentRun(int& runnum, int* runconfig=nullptr, std::string* err=nullptr);
	bool GetRunConfig(int& runconfig, int* runnum_in=nullptr, std::string* err=nullptr);
	bool GetSystemConfig(int& systemconfig, std::string* systemname_in=nullptr, int* runnum_in=nullptr, int* runconfig_in=nullptr, std::string* err=nullptr);
	bool GetToolsConfig(std::string& toolsconfig, std::string* systemname_in=nullptr, int* runnum_in=nullptr, int* runconfig_in=nullptr, int* systemconfig_in=nullptr, std::string* err=nullptr);
	bool GetToolConfig(std::string toolname, std::string& toolconfig, std::string* systemname_in=nullptr, int* runnum_in=nullptr, int* runconfig_in=nullptr, int* systemconfig_in=nullptr, std::string* err=nullptr);
	bool GetToolConfig(std::string& toolconfig, std::string systemname, std::string className, double version, std::string* err=nullptr);
	bool ParseToolsConfig(std::string toolsconfig, BStore& configStore, std::string* err); ///< parse the toolsconfig string into a Store where key is the unique toolname and value is a std::pair< std::string ToolClassName, int ToolConfigFileVersion >
	
	int InsertToolConfig(Store config, std::string toolname, std::string author, std::string description, std::string* systemname_in=nullptr, std::string* err=nullptr);
	
	private:
	DataModel* m_data;
	
	int verbosity=1;
	int v_error=0;
	int v_warning=1;
	int v_message=2;
	int v_debug=3;
	std::string logmessage;
	int get_ok;
	
};


#endif
