package Classes;

public final class Parameters {

	static final int timeUnits = 1000;

	// resource consumption going from client to web server
	static final int clientToWS_mips = 300 * timeUnits / 10;
	static final int clientToWS_iops = 1;
	static final double clientToWS_ram = 1;// 500
	static final int clientToWS_transferData = 1 * timeUnits;
	// resource consumption going from ES to Web Server
	static final int ESToWS_mips = 300 * timeUnits / 10;
	static final int ESToWS_iops = 1;
	static final double ESToWS_ram = 1;// 500
	static final int ESToWS_transferData = 1 * timeUnits;

	// resource consumption going from web server to ES
	static final int WSToES_mips = 300 * timeUnits / 10;
	static final int WSToES_iops = 1;
	static final double WSToES_ram = 200;// 500
	static final int WSToES_transferData = 1 * timeUnits;
	// resource consumption going from DataNode to ES
	static final int DNToES_mips = 300 * timeUnits / 10;
	static final int DNToES_iops = 1;
	static final double DNToES_ram = 1;// 1000
	static final int DNToES_transferData = 1 * timeUnits;

	// resource consumption going from ES to DataNode
	static final int ESToDN_mips = 1 * timeUnits / 10;
	static final int ESToDN_iops = 1;
	static final double ESToDN_ram = 1; // 2000
	static final int ESToDN_transferData = 1 * timeUnits;
	// resource consumption from datanode to datanode
	static final int DNToDN_mips = 300 * timeUnits / 10;
	static final int DNToDN_iops = 1;
	static final double DNToDN_ram = 1;// 1000
	static final int DNToDN_transferData = 1 * timeUnits;
	
	

}
