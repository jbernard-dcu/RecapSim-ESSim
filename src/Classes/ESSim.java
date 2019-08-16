package Classes;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel.Unit;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.listeners.CloudletVmEventInfo;

import eu.recap.sim.RecapSim;
import eu.recap.sim.cloudsim.cloudlet.IRecapCloudlet;
import eu.recap.sim.cloudsim.cloudlet.RecapCloudlet;
import eu.recap.sim.cloudsim.vm.IRecapVe;
import eu.recap.sim.cloudsim.vm.RecapVe;
import eu.recap.sim.helpers.Log;
import eu.recap.sim.helpers.ModelHelpers;
import eu.recap.sim.models.ApplicationModel.Application.Component.Api;
import eu.recap.sim.models.InfrastructureModel.Link;
import eu.recap.sim.models.WorkloadModel.Request;

public class ESSim extends RecapSim {

	public static void main(String[] args) {

	}

	public ESSim() {
		super();
	}

	@Override
	protected IRecapCloudlet createCloudlet(Vm vm, long mi, long inputFileSize, long outputFileSize, long io,
			double submissionDelay, String applicationId, String componentId, String apiId, double ram_cloudlet,
			int requestId, String originDeviceId) {
		// final long length = 10000; //in Million Structions (MI)
		// final long fileSize = 300; //Size (in bytes) before execution
		// final long outputSize = 300; //Size (in bytes) after execution
		int numberOfCpuCores = (int) vm.getNumberOfPes(); // cloudlet will
															// use all the
															// VM's CPU
															// cores
		numberOfCpuCores = 1;

		// Defines how CPU, RAM and Bandwidth resources are used
		// Sets the same utilization model for all these resources.
		UtilizationModel utilization = new UtilizationModelFull();

		// Default ElasticSearch setting here
		UtilizationModel uRamES = new UtilizationModelDynamic(Unit.PERCENTAGE, 50);

		UtilizationModel utilizationCpu = /* utilization;// */new UtilizationModelDynamic(Unit.ABSOLUTE,
				RecapSim.getUmCpuValue());
		UtilizationModel utilizationRam = /* uRamES;// */new UtilizationModelDynamic(Unit.ABSOLUTE, ram_cloudlet);
		UtilizationModel utilizationBw = new UtilizationModelFull();

//		IRecapCloudlet recapCloudlet = (IRecapCloudlet) new RecapCloudlet(cloudletList.size(), mi, numberOfCpuCores)
//				.setFileSize(inputFileSize).setOutputSize(outputFileSize).setUtilizationModel(new UtilizationModelDynamic(0.1)).setVm(vm)
//				.addOnFinishListener(this::onCloudletFinishListener);

		IRecapCloudlet recapCloudlet = (IRecapCloudlet) new RecapCloudlet(cloudletList.size(), mi, numberOfCpuCores)
				.setFileSize(inputFileSize).setOutputSize(outputFileSize).setUtilizationModelCpu(utilizationCpu)
				.setUtilizationModelRam(utilizationRam).setUtilizationModelBw(utilizationBw).setVm(vm)
				.addOnFinishListener(this::onCloudletFinishListener);

		recapCloudlet.setSubmissionDelay(submissionDelay);
		recapCloudlet.setApplicationId(applicationId);
		recapCloudlet.setApplicationComponentId(componentId);
		recapCloudlet.setApiId(apiId);
		recapCloudlet.setRequestId(requestId);
		recapCloudlet.setOriginDeviceId(originDeviceId);

		return recapCloudlet;
	}

	@Override
	protected void onCloudletFinishListener(CloudletVmEventInfo eventInfo) {

		// 0. Bug workaround: check if it is a second execution of the listener
		// If the entry already here then we skip it
		if (this.finishedCloudlets.containsKey(eventInfo.getCloudlet().getId())) {

			// Log.printFormattedLine("\n#Bugfix#All following entries already
			// executed for CloudletId:"+eventInfo.getCloudlet().getId()+"\n");

		} else {
			IRecapCloudlet finishedRecapCloudlet = (IRecapCloudlet) eventInfo.getCloudlet();
			RecapVe currentVe = (RecapVe) eventInfo.getVm();

			Log.printFormattedLine("Finished ApplicationId:" + finishedRecapCloudlet.getApplicationId()
					+ " ComponentId:" + finishedRecapCloudlet.getApplicationComponentId() + " apiTaskId: "
					+ finishedRecapCloudlet.getApiId());

			Log.printFormattedLine("\n#EventListener: Cloudlet %d finished running at Vm %d at time %.2f",
					finishedRecapCloudlet.getId(), currentVe.getId(), eventInfo.getTime());

			// 1. Check if the Request that has triggered the VM has next
			// cloudlet to execute
			Api currentApi = ModelHelpers.getApiTask(this.ram.getApplicationsList(),
					finishedRecapCloudlet.getApplicationId(), finishedRecapCloudlet.getApplicationComponentId(),
					finishedRecapCloudlet.getApiId());

			// check if we have a chain of application compoents attached
			if (!currentApi.getNextApiIdList().isEmpty() && !currentApi.getNextComponentIdList().isEmpty()) {
				Log.printFormattedLine("Found next component IDs:" + currentApi.getNextComponentIdList());
				Log.printFormattedLine("            with API IDs:" + currentApi.getNextApiIdList());

				// case if we only have one next API
				if (currentApi.getNextApiIdList().size() == 1 && currentApi.getNextComponentIdList().size() == 1) {
					Api nextApi = ModelHelpers.getApiTask(this.ram.getApplicationsList(),
							finishedRecapCloudlet.getApplicationId(), currentApi.getNextComponentId(0),
							currentApi.getNextApiId(0));

					Request request = ModelHelpers.getRequestTask(this.rwm.getDevicesList(),
							finishedRecapCloudlet.getOriginDeviceId(), finishedRecapCloudlet.getRequestId());
					// if next API is DataNode To ES => need to aggregate the APIs
					if (nextApi.getApiId().equals("2_2")) {
						if (!executedDataNodesPerRequest.containsKey(request.getRequestId())) {
							executedDataNodesPerRequest.put(request.getRequestId(), 1);
						} else {
							executedDataNodesPerRequest.put(request.getRequestId(),
									executedDataNodesPerRequest.get(request.getRequestId()) + 1);
						}
					}

					if ((nextApi.getApiId().equals("2_2")
							&& executedDataNodesPerRequest.get(request.getRequestId()) == request.getDataNodesCount())
							|| (!nextApi.getApiId().equals("2_2"))) {
						Log.printFormattedLine("Create one new cloudlet");

						// 2. Create cloudlet using api specs
						double delay = 0.0;
						IRecapVe targetVe = getMatchingVeId(finishedRecapCloudlet.getApplicationId(),
								currentApi.getNextComponentId(0));
						// 2b. create cloudlet

						IRecapCloudlet newRecapCloudlet = createCloudlet(targetVe, nextApi.getMips(),
								nextApi.getDataToTransfer(), nextApi.getDataToTransfer(), nextApi.getIops(), delay,
								finishedRecapCloudlet.getApplicationId(), currentApi.getNextComponentId(0),
								nextApi.getApiId(), nextApi.getRam(), finishedRecapCloudlet.getRequestId(),
								finishedRecapCloudlet.getOriginDeviceId());
						newRecapCloudlet.setBwUpdateTime(simulation.clock());

						// 2a. calculate delay based on the connection
						// TO-DO: Update transfer remaining speeds when a cloudlet finished transferring
						// through a link
						// is cloudlet being sent between DC sites?
						if (targetVe.getHost().getDatacenter().getId() != currentVe.getHost().getDatacenter().getId()) {
							// if so calculate link BW demand
							Link link = ModelHelpers.getNetworkLink(rim, currentVe.getHost().getDatacenter().getName(),
									targetVe.getHost().getDatacenter().getName());
							int linkBw = link.getBandwith();

							// get current cloudlets on the link
							List<IRecapCloudlet> listActivecloudlets;
							if (activeLinkCloudlets.containsKey(link.getId())) {

								listActivecloudlets = activeLinkCloudlets.get(link.getId());

								// clean cloudlets list that are being processed already. Cloudlets that are not
								// in status instantiated are removed from the list
								for (IRecapCloudlet cl : listActivecloudlets) {
									if (!cl.getStatus()
											.equals(org.cloudbus.cloudsim.cloudlets.Cloudlet.Status.INSTANTIATED)) {
										listActivecloudlets.remove(cl);
									}
								}
								listActivecloudlets.add(newRecapCloudlet);
								// update
								activeLinkCloudlets.put(link.getId(), listActivecloudlets);

							} else {
								// create list and add the cloudlet
								listActivecloudlets = new ArrayList<IRecapCloudlet>();
								listActivecloudlets.add(newRecapCloudlet);
								activeLinkCloudlets.put(link.getId(), listActivecloudlets);
							}

							// assume bandwidth divided equally
							double availableBandwithSliceForCloudlet = linkBw / listActivecloudlets.size();

							// bandwith speed is in Megabits per second where file size is in Bytes, so we
							// convert Megabits to Bytes by multiplying by 125000
							// calculate delay Megabits Bytes
							double ByteperSecond = 125000 * availableBandwithSliceForCloudlet;
							delay = ByteperSecond / newRecapCloudlet.getFileSize();
							newRecapCloudlet.setSubmissionDelay(delay);

							// Update the delay for the rest of cloudlets in the list based on more
							// cloudlets in the link
							// check if more cloudlets in the list than the new one
							if (listActivecloudlets.size() > 1) {
								// calculate how much of data was already transferred in the previous time slice
								// update with new delays for the remainder of the data to be transferred
								for (IRecapCloudlet cl : listActivecloudlets) {
									// all except the new one
									if (cl.getId() != newRecapCloudlet.getId()) {
										double timePassedInDataTransfer = simulation.clock() - cl.getBwUpdateTime();
										// calculate already how much was transferred
										double availableBandwithSliceBeforeNewVM = linkBw
												/ (listActivecloudlets.size() - 1);
										double transferredBytes = cl.getFileSize() - (timePassedInDataTransfer
												* (availableBandwithSliceBeforeNewVM * 125000));
										// new delay with new slice byteper second
										double newDelay = ByteperSecond / (cl.getFileSize() - transferredBytes);
										cl.setSubmissionDelay(newDelay);
										// set the bytes that were transferred in the past time and time when that was
										// updated before the new time delay estimation
										cl.setTransferredBytes(transferredBytes);
										cl.setBwUpdateTime(simulation.clock());

									}
								}

							}

						}

						// need to add cloudlet to the list to have a consistent ID
						cloudletList.add(newRecapCloudlet);
						onTheFlycloudletList.add(newRecapCloudlet);
						Log.printFormattedLine("Submitting Cloudlet ID: " + newRecapCloudlet.getId());
						this.broker0.submitCloudlet(newRecapCloudlet);
						System.out.println("#Submittedcl " + newRecapCloudlet.getStatus());

						// System.out.println("#FinishedCL: "+eventInfo.getCloudlet().getStatus());

						// add the key to the check list
						finishedCloudlets.put(eventInfo.getCloudlet().getId(), true);

						this.broker0.getCloudletWaitingList();

					}

				}

				else // case if we have multiple next APIs
				{
					Log.printFormattedLine("Create many new cloudlets");

					Request request = ModelHelpers.getRequestTask(this.rwm.getDevicesList(),
							finishedRecapCloudlet.getOriginDeviceId(), finishedRecapCloudlet.getRequestId());

					for (int positionNexApi = 0; positionNexApi < currentApi.getNextApiIdCount(); positionNexApi++) {
						if (!request.getDataNodesList().contains(positionNexApi + 1)) {
							continue;
						}

						Api nextApi = ModelHelpers.getApiTask(this.ram.getApplicationsList(),
								finishedRecapCloudlet.getApplicationId(), currentApi.getNextComponentId(positionNexApi),
								currentApi.getNextApiId(positionNexApi));

						// 2. Create cloudlet using api specs
						double delay = 0.0;
						IRecapVe targetVe = getMatchingVeId(finishedRecapCloudlet.getApplicationId(),
								currentApi.getNextComponentId(positionNexApi));
						// 2b. create cloudlet

						// Updated MIPS DataNodes here

						IRecapCloudlet newRecapCloudlet = createCloudlet(targetVe, request.getMipsDataNodes(),
								nextApi.getDataToTransfer(), nextApi.getDataToTransfer(), nextApi.getIops(), delay,
								finishedRecapCloudlet.getApplicationId(), currentApi.getNextComponentId(positionNexApi),
								nextApi.getApiId(), nextApi.getRam(), finishedRecapCloudlet.getRequestId(),
								finishedRecapCloudlet.getOriginDeviceId());
						newRecapCloudlet.setBwUpdateTime(simulation.clock());

						// 2a. calculate delay based on the connection
						// TO-DO: Update transfer remaining speeds when a cloudlet finished transferring
						// through a link
						// is cloudlet being sent between DC sites?
						if (targetVe.getHost().getDatacenter().getId() != currentVe.getHost().getDatacenter().getId()) {
							// if so calculate link BW demand
							Link link = ModelHelpers.getNetworkLink(rim, currentVe.getHost().getDatacenter().getName(),
									targetVe.getHost().getDatacenter().getName());
							int linkBw = link.getBandwith();

							// get current cloudlets on the link
							List<IRecapCloudlet> listActivecloudlets;
							if (activeLinkCloudlets.containsKey(link.getId())) {

								listActivecloudlets = activeLinkCloudlets.get(link.getId());

								// clean cloudlets list that are being processed already. Cloudlets that are not
								// in status instantiated are removed from the list
								for (IRecapCloudlet cl : listActivecloudlets) {
									if (!cl.getStatus()
											.equals(org.cloudbus.cloudsim.cloudlets.Cloudlet.Status.INSTANTIATED)) {
										listActivecloudlets.remove(cl);
									}
								}
								listActivecloudlets.add(newRecapCloudlet);
								// update
								activeLinkCloudlets.put(link.getId(), listActivecloudlets);

							} else {
								// create list and add the cloudlet
								listActivecloudlets = new ArrayList<IRecapCloudlet>();
								listActivecloudlets.add(newRecapCloudlet);
								activeLinkCloudlets.put(link.getId(), listActivecloudlets);
							}

							// assume bandwidth divided equally
							double availableBandwithSliceForCloudlet = linkBw / listActivecloudlets.size();

							// bandwith speed is in Megabits per second where file size is in Bytes, so we
							// convert Megabits to Bytes by multiplying by 125000
							// calculate delay Megabits Bytes
							double ByteperSecond = 125000 * availableBandwithSliceForCloudlet;
							delay = ByteperSecond / newRecapCloudlet.getFileSize();

							System.out.println("DELAY:" + delay);

							newRecapCloudlet.setSubmissionDelay(delay);

							// Update the delay for the rest of cloudlets in the list based on more
							// cloudlets in the link
							// check if more cloudlets in the list than the new one
							if (listActivecloudlets.size() > 1) {
								// calculate how much of data was already transferred in the previous time slice
								// update with new delays for the remainder of the data to be transferred
								for (IRecapCloudlet cl : listActivecloudlets) {
									// all except the new one
									if (cl.getId() != newRecapCloudlet.getId()) {
										double timePassedInDataTransfer = simulation.clock() - cl.getBwUpdateTime();
										// calculate already how much was transferred
										double availableBandwithSliceBeforeNewVM = linkBw
												/ (listActivecloudlets.size() - 1);
										double transferredBytes = cl.getFileSize() - (timePassedInDataTransfer
												* (availableBandwithSliceBeforeNewVM * 125000));
										// new delay with new slice byteper second
										double newDelay = ByteperSecond / (cl.getFileSize() - transferredBytes);
										cl.setSubmissionDelay(newDelay);
										// set the bytes that were transferred in the past time and time when that was
										// updated before the new time delay estimation
										cl.setTransferredBytes(transferredBytes);
										cl.setBwUpdateTime(simulation.clock());

									}
								}

							}

						}

						// need to add cloudlet to the list to have a consistent ID
						cloudletList.add(newRecapCloudlet);
						onTheFlycloudletList.add(newRecapCloudlet);
						Log.printFormattedLine("Submitting Cloudlet ID: " + newRecapCloudlet.getId());
						this.broker0.submitCloudlet(newRecapCloudlet);
						System.out.println("#Submittedcl " + newRecapCloudlet.getStatus());

						// System.out.println("#FinishedCL: "+eventInfo.getCloudlet().getStatus());

						// add the key to the check list
						finishedCloudlets.put(eventInfo.getCloudlet().getId(), true);

						this.broker0.getCloudletWaitingList();
					}

				}

			}

		}

	}

}
