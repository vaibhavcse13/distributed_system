package com.vaibhav.serviceregistry.cluster.management;

public interface OnElectionCallback {

    void onElectedToBeLeader();

    void onWorker();
}
