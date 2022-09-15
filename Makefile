
all: main

.phony: clean

Dependencies=/home/NewDAQ/Dependencies

StoreInclude= -I $(Dependencies)/ToolFrameworkCore/include
StoreLib= -L $(Dependencies)/ToolFrameworkCore/lib -lStore

#  ServiceDiscovery, DAQUtilities, Logging
DAQUtilitiesInclude= -I $(Dependencies)/ToolDAQFramework/include
DAQUtilitiesLib= -L $(Dependencies)/ToolDAQFramework/lib -lServiceDiscovery -lDataModel -lDAQLogging -lLogging

BoostLib= -L $(Dependencies)/boost_1_66_0/install/lib -lboost_date_time -lboost_serialization -lboost_iostreams -lboost_system
BoostInclude= -I $(Dependencies)/boost_1_66_0/install/include

ZMQLib= -L $(Dependencies)/zeromq-4.0.7/lib -lzmq 
ZMQInclude= -I $(Dependencies)/zeromq-4.0.7/include/

main: minimaltester.cpp PGClient.cpp DataModel.cpp PGHelper.cpp DataModel.h PGHelper.h
	g++ -g -fdiagnostics-color=always -std=c++11 -lpthread -Wno-psabi -Wno-attributes minimaltester.cpp PGClient.cpp PGHelper.cpp DataModel.cpp -I ./ $(DAQUtilitiesLib) $(DAQUtilitiesInclude) $(BoostInclude) $(ZMQInclude) $(StoreInclude) $(BoostLib) $(ZMQLib) $(StoreLib) -o $@

clean:
	rm -f *.o main
