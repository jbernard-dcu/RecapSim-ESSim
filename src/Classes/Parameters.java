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
	static final double WSToES_ram = 1;// 500
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

	final static List<Pair<path, typeParam>> apiParamKeyset = buildApiParamKeyset();
	final static Map<Pair<path, typeParam>, Number> apiParam = buildApiParamMap();

	private static Map<Pair<path, typeParam>, Number> buildApiParamMap() {
		Map<Pair<path, typeParam>, Number> mapParam = new HashMap<Pair<path, typeParam>, Number>();

		mapParam.put(apiParamKeyset.get(0), clientToWS_mips);
		mapParam.put(apiParamKeyset.get(1), clientToWS_iops);
		mapParam.put(apiParamKeyset.get(2), clientToWS_ram);
		mapParam.put(apiParamKeyset.get(3), clientToWS_transferData);
		mapParam.put(apiParamKeyset.get(4), ESToWS_mips);
		mapParam.put(apiParamKeyset.get(5), ESToWS_iops);
		mapParam.put(apiParamKeyset.get(6), ESToWS_ram);
		mapParam.put(apiParamKeyset.get(7), ESToWS_transferData);
		mapParam.put(apiParamKeyset.get(8), WSToES_mips);
		mapParam.put(apiParamKeyset.get(9), WSToES_iops);
		mapParam.put(apiParamKeyset.get(10), WSToES_ram);
		mapParam.put(apiParamKeyset.get(11), WSToES_transferData);
		mapParam.put(apiParamKeyset.get(12), DNToES_mips);
		mapParam.put(apiParamKeyset.get(13), DNToES_iops);
		mapParam.put(apiParamKeyset.get(14), DNToES_ram);
		mapParam.put(apiParamKeyset.get(15), DNToES_transferData);
		mapParam.put(apiParamKeyset.get(16), ESToDN_mips);
		mapParam.put(apiParamKeyset.get(17), ESToDN_iops);
		mapParam.put(apiParamKeyset.get(18), ESToDN_ram);
		mapParam.put(apiParamKeyset.get(19), ESToDN_transferData);
		mapParam.put(apiParamKeyset.get(20), DNToDN_mips);
		mapParam.put(apiParamKeyset.get(21), DNToDN_iops);
		mapParam.put(apiParamKeyset.get(22), DNToDN_ram);
		mapParam.put(apiParamKeyset.get(23), DNToDN_transferData);

		return mapParam;
	}

	private static List<Pair<path, typeParam>> buildApiParamKeyset() {
		List<Pair<path, typeParam>> res = new ArrayList<>();
		for (path path : path.class.getEnumConstants()) {
			for (typeParam type : typeParam.class.getEnumConstants()) {
				res.add(new Pair<path, typeParam>(path, type));
			}
		}
		return res;
	}

	/**
	 * returns the key corresponding to the specified path and type
	 * 
	 * @param path
	 * @param type
	 * @return
	 */
	private static Pair<path, typeParam> getKey(path path, typeParam type) {
		for (Pair<path, typeParam> key : apiParamKeyset) {
			if (key.getKey() == path && key.getValue() == type)
				return key;
		}
		return null;
	}

	/**
	 * Returns the specified path and type parameter
	 * 
	 * @param type
	 * @param path
	 * @return
	 */
	public static Number getParam(typeParam type, path path) {
		return apiParam.get(getKey(path, type));
	}

	/**
	 * Returns the list of parameters of the specified type
	 * 
	 * @param type
	 * @return
	 */
	public static List<Number> getListByType(typeParam type) {
		List<Number> res = new ArrayList<Number>();
		for (path path : path.class.getEnumConstants()) {
			res.add(apiParam.get(getKey(path, type)));
		}
		return res;
	}

	/**
	 * Returns the list of parameters of the specified path
	 * 
	 * @param path
	 * @return
	 */
	public static List<Number> getListByPath(path path) {
		List<Number> res = new ArrayList<Number>();
		for (typeParam type : typeParam.class.getEnumConstants()) {
			res.add(apiParam.get(getKey(path, type)));
		}
		return res;
	}

	/**
	 * Sets the mips, iops, ram and dataToTransfer of the specified api given the
	 * static parameters in this class
	 * 
	 * @param api
	 * @param apiPath
	 * @return
	 */
	public static Api.Builder setParamsApi(Api.Builder api, path apiPath) {
		List<Number> params = getListByPath(apiPath);
		api.setMips(params.get(0).intValue());
		api.setIops(params.get(1).intValue());
		api.setRam(params.get(2).doubleValue());
		api.setDataToTransfer(params.get(3).longValue());
		return api;
	}

}
