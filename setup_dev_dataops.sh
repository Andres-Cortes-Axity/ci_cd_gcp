#!/bin/bash

# Save project ID
PROJECT_ID=$(gcloud config get-value project)




# Enabling API services
services=(
    "cloudbuild.googleapis.com"
    "storage.googleapis.com"
    "bigquery.googleapis.com"
    "dataproc.googleapis.com"
    "iam.googleapis.com"
)

for service in "${services[@]}"
do
    echo "Enabling $service"
    gcloud services enable $service
done



# Service accounts, here you must configure your respective service accounts
SA_QA="70177019171@cloudbuild.gserviceaccount.com"
COMPUTE_SA="70177019171-compute@developer.gserviceaccount.com"
DATAPROC_SA="service-70177019171@dataproc-accounts.iam.gserviceaccount.com"

# Roles to assign to SA_QA service account
sa_roles=(
    "roles/storage.admin"
    "roles/bigquery.admin"
    "roles/bigquery.jobUser"
    "roles/dataproc.editor"
    "roles/bigquery.dataEditor"
    "roles/cloudbuild.serviceAgent"
    "roles/iam.serviceAccountUser"
    "roles/storage.objectViewer"
    "roles/storage.objectCreator"
)

# The following roles are assigned to the Compute Service Account (COMPUTE_SA) 
# to enable Dataproc job execution and facilitate internal and external API 
# interactions within the Google Cloud Platform (GCP) environment.
compute_sa_roles=(
    "roles/bigquery.dataEditor"
    "roles/bigquery.jobUser"
    "roles/dataproc.worker" 
    "roles/dataproc.serviceAgent"
    "roles/logging.logWriter"
    "roles/monitoring.metricWriter"
    "roles/storage.objectViewer"
    "roles/storage.objectCreator"
    "roles/compute.networkAdmin"
    "roles/compute.securityAdmin"
)

#Dataproc service agent roles
dataproc_sa_roles=(
    "roles/storage.objectViewer"
    "roles/storage.objectCreator"
    "roles/storage.admin"

)

# Assign roles to cloud build service account
for role in "${sa_roles[@]}"
do
    echo "Assigning $role to $SA_QA"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SA_QA" \
        --role="$role" \
        --condition=None
    
    if [ $? -eq 0 ]; then
        echo "Successfully assigned $role to $SA_QA"
    else
        echo "Failed to assign $role to $SA_QA"
    fi
done

# Assign roles to Compute service account
for role in "${compute_sa_roles[@]}"
do
    echo "Assigning $role to Compute service account $COMPUTE_SA"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="$role" \
        --condition=None
    
    if [ $? -eq 0 ]; then
        echo "Successfully assigned $role to $COMPUTE_SA"
    else
        echo "Failed to assign $role to $COMPUTE_SA"
    fi
done

#Assign roles to Dataproc service agent
for role in "${dataproc_sa_roles[@]}"
do
    echo "Assigning $role to Dataproc service agent $DATAPROC_SA"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$DATAPROC_SA" \
        --role="$role" \
        --condition=None
    
    if [ $? -eq 0 ]; then
        echo "Successfully assigned $role to $DATAPROC_SA"
    else
        echo "Failed to assign $role to $DATAPROC_SA"
    fi
done


# Create project bucket if it doesn't exist
BUCKET_NAME="${PROJECT_ID}-datalake"
if gsutil ls -b gs://${BUCKET_NAME} > /dev/null 2>&1; then
    echo "Bucket gs://${BUCKET_NAME} already exists. Skipping creation."
else
    echo "Creating bucket gs://${BUCKET_NAME}"
    gsutil mb gs://${BUCKET_NAME}
fi

# Variables
REGION="us-east4"
VPC_NETWORK="default"

# Enable Private Google Access on the default subnet
echo "Enabling Private Google Access on default subnet in ${REGION}..."
gcloud compute networks subnets update default \
    --region=${REGION} \
    --enable-private-ip-google-access \
    --project=${PROJECT_ID}

# Set up Cloud NAT for external connectivity
echo "Setting up Cloud Router..."
if ! gcloud compute routers describe dataproc-router --region=$REGION --project=$PROJECT_ID &>/dev/null; then
    gcloud compute routers create dataproc-router \
        --network=$VPC_NETWORK \
        --region=$REGION \
        --project=$PROJECT_ID
    echo "Cloud Router created successfully"
else
    echo "Cloud Router already exists"
fi

echo "Setting up Cloud NAT..."
if ! gcloud compute routers nats describe dataproc-nat --router=dataproc-router --region=$REGION --project=$PROJECT_ID &>/dev/null; then
    gcloud compute routers nats create dataproc-nat \
        --router=dataproc-router \
        --region=$REGION \
        --auto-allocate-nat-external-ips \
        --nat-all-subnet-ip-ranges \
        --enable-logging \
        --project=$PROJECT_ID
    echo "Cloud NAT created successfully"
else
    echo "Cloud NAT already exists"
fi

# Create firewall rule for external access
echo "Creating firewall rule for external access..."
if ! gcloud compute firewall-rules describe allow-internet-egress --project=$PROJECT_ID &>/dev/null; then
    gcloud compute firewall-rules create allow-internet-egress \
        --direction=EGRESS \
        --priority=1000 \
        --network=$VPC_NETWORK \
        --action=ALLOW \
        --rules=tcp:80,tcp:443 \
        --destination-ranges=0.0.0.0/0 \
        --project=$PROJECT_ID
    echo "Firewall rule created successfully"
else
    echo "Firewall rule already exists"
fi

echo "Setup complete!"