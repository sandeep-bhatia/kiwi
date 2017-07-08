#Sandeeps-MBP:kiwi Sandeep$ clusterName="$(aws emr list-clusters --query 'Clusters[*].[Id]' --output text)"
#Sandeeps-MBP:kiwi Sandeep$ echo "${clusterName}"
echo "This script will create an EMR cluster, monitor its execution and terminate it when the work is done"
#Create a spark cluster 
#Copy relevant files to the cluster
#Use the key and command line ssh to connect to the cluster 
#run the jobs on the cluster 
#start the termination of the cluster after ssh exits 
#do this periodically based on the pipeline requirements
chmod 777 *

echo "Copy the cluster key to the current directory"
aws s3 cp s3://datomata.production.jars/DatomataClusterKeyPair.pem .

echo "Creating the cluster now"
clusterName="$(aws emr create-cluster --name "Sort Pipeline EMR cluster"  --release-label emr-4.2.0 --applications Name=Spark --ec2-attributes KeyName=DatomataClusterKeyPair --instance-type m3.xlarge --instance-count 4 --use-default-roles --query 'ClusterId' --output text)"
echo "Cluster with SPARK installation created"
echo "${clusterName}"
state="$(aws emr describe-cluster --cluster-id "${clusterName}" --query 'Cluster.Status.State')"
chmod 600 *
echo "wait for the cluster to be up now ..."
while [ $state == '"STARTING"' ]; do echo "Waiting for the cluster to be up....."; echo "current state.."; echo $state; state="$(aws emr describe-cluster --cluster-id "${clusterName}" --query 'Cluster.Status.State')"; sleep 10; done
aws emr ssh --cluster-id "${clusterName}" --key-pair-file ./DatomataClusterKeyPair.pem	<<'ENDSSH'
cd ~
mkdir datomata
cd datomata
aws s3 cp s3://datomata.production.jars/Kiwi-1.0.0.jar .
spark-submit --class kiwi.Knitter ./Kiwi-1.0.0.jar _ PunjabKesari sort
ENDSSH
chmod 777 *
echo "Completed the run job, terminating the cluster now"
echo ${clusterName}
aws emr terminate-clusters --cluster-ids ${clusterName}



