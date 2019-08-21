package Classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import eu.recap.sim.models.ApplicationModel.Application.Component.Api;

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

	// Map of values for getter
	public static enum typeParam {
		mips, iops, ram, transferData
	}

	public static enum path {
		clientToWS, ESToWS, WSToES, DNToES, ESToDN, DNToDN
	}

	final static Map<Pair<path, typeParam>, Number> mapParams = buildMapParam();

	private static Map<Pair<path, typeParam>, Number> buildMapParam() {
		Map<Pair<path, typeParam>, Number> mapParam = new HashMap<Pair<path, typeParam>, Number>();

		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), clientToWS_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), clientToWS_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), clientToWS_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), clientToWS_transferData);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), ESToWS_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), ESToWS_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), ESToWS_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), ESToWS_transferData);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), WSToES_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), WSToES_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), WSToES_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), WSToES_transferData);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), DNToES_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), DNToES_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), DNToES_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), DNToES_transferData);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), ESToDN_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), ESToDN_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), ESToDN_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), ESToDN_transferData);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.mips), DNToDN_mips);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.iops), DNToDN_iops);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.ram), DNToDN_ram);
		mapParam.put(new Pair<path, typeParam>(path.clientToWS, typeParam.transferData), DNToDN_transferData);

		return mapParam;
	}

	public static Number getParam(typeParam type, path path) {
		return mapParams.get(new Pair<path, typeParam>(path, type));
	}

	public static List<Number> getListByType(typeParam type) {
		List<Number> res = new ArrayList<Number>();
		for (path path : path.class.getEnumConstants()) {
			res.add(mapParams.get(new Pair<path, typeParam>(path, type)));
		}
		return res;
	}

	public static List<Number> getListByPath(path path) {
		List<Number> res = new ArrayList<Number>();
		for (typeParam type : typeParam.class.getEnumConstants()) {
			res.add(mapParams.get(new Pair<path, typeParam>(path, type)));
		}
		return res;
	}
	
	public static Api.Builder setParamsApi(Api.Builder api, path apiPath){
		List<Number> params = getListByPath(apiPath);
		api.setMips((int)params.get(0));
		api.setIops((int)params.get(1));
		api.setRam((double)params.get(2));
		api.setDataToTransfer((long)params.get(3));
		return api;
	}

}
