import streamlit as st
import pandas as pd
import re
import boto3
import hashlib
from io import StringIO, BytesIO
import os
import time
import datetime

#Get AWS Credentials
aws_credentials = st.secrets["aws"]
aws_id = aws_credentials["AWS_ACCESS_KEY_ID"]
access_key  = aws_credentials["AWS_SECRET_ACCESS_KEY"]
region = aws_credentials["REGION"]
account_id = aws_credentials["ACCOUNT_ID"]
session = boto3.Session(aws_access_key_id = aws_id, aws_secret_access_key = access_key, region_name = region, aws_account_id = account_id )

# Initialize DynamoDB client
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('PK_Hash_Index')

def hash_of(nid):
    return hashlib.sha256(nid.encode()).hexdigest()

def check_dynamodb_for_duplicates(normalized_ids):
    duplicates = []
    for nid in normalized_ids:
        hashed_nid = hash_of(nid)
        response = table.get_item(Key={'PK_Hash': hashed_nid})
        if 'Item' in response:
            duplicates.append(nid)
    return duplicates

def clean_filename(name):
    return re.sub(r"[^\w!\-_.\*'()]", '_', name)

def upload_to_s3(file_dataframes, metadata_normalized_ids):
    s3_client = session.client('s3')
    bucket_mapping = {
        "bacterial-abundance": "mmrc-bacterial-abundance",
        "bacterial-pathway": "mmrc-bacterial-pathway",
        "enzymes-abundance": "mmrc-enzymes-abundance",
        "nmr-asics-abundance": "mmrc-nmr-asics-abundance",
        "participant-metadata": "mmrc-participant-metadata",
        "new-taxonomy-IDs": "mmrc-taxonomy-tables"
    }

    all_files_uploaded = True
    
    # Create a mapping from our labels to the original filenames
    label_to_original_name = {}
    for label, file in uploaded_files.items():
        label_to_original_name[label] = file.name  # Store original filename
    
    for label, df in file_dataframes.items():
        bucket_name = bucket_mapping.get(label)
        if not bucket_name:
            st.error(f"No S3 bucket defined for `{label}`.")
            all_files_uploaded = False
            continue

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        file_obj = BytesIO(csv_buffer.getvalue().encode('utf-8'))
        
        # Use the original filename if available, otherwise fall back to label
        original_filename = label_to_original_name.get(label, f"{label}.csv")
        cleaned_filename = clean_filename(os.path.splitext(original_filename)[0])
        extension = os.path.splitext(original_filename)[1]
        # timestamp_hash = hashlib.sha256(str(time.time()).encode()).hexdigest()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"raw/{cleaned_filename}_{timestamp}{extension}"

        try:
            file_obj.seek(0)
            s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
            st.success(f"Uploaded `{original_filename}` as `{s3_key}` to `{bucket_name}`.")
        except Exception as e:
            all_files_uploaded = False
            st.error(f"Failed to upload `{original_filename}` to `{bucket_name}`: {str(e)}")

    if all_files_uploaded and metadata_normalized_ids:
        update_dynamodb(metadata_normalized_ids)
        # Clear session state after successful upload
        st.session_state.clear()  # This resets the entire session

def update_dynamodb(metadata_normalized_ids):
    for nid in metadata_normalized_ids:
        hashed_nid = hash_of(nid)
        table.put_item(Item={'PK_Hash': hashed_nid})
    st.success("DynamoDB updated with new normalized IDs.")

def get_normalized_id(raw_id):
    """Extracts digits and removes leading zeros for consistent comparison"""
    digits = re.sub(r'\D', '', str(raw_id))
    return digits.lstrip('0') or '0'  # Preserve single zero if all zeros

# Add this function to track file upload states
def get_file_signatures(uploaded_files):
    """Generate unique signatures for each uploaded file"""
    return {name: hashlib.md5(file.getvalue()).hexdigest() for name, file in uploaded_files.items()}

# Add this function to count unique records
def count_unique_records(file_dataframes, required_only=False):
    """Count unique normalized IDs across all dataframes"""
    unique_ids = set()
    for label, df in file_dataframes.items():
        if not required_only or label in REQUIRED_FILES:
            sample_ids = df.iloc[:, 0].apply(get_normalized_id)
            unique_ids.update(sample_ids)
    return len(unique_ids)

st.title("Data Upload Portal")

REQUIRED_FILES = [
    "bacterial-abundance",
    "bacterial-pathway",
    "enzymes-abundance",
    "nmr-asics-abundance",
    "participant-metadata"
]

OPTIONAL_FILES = ["new-taxonomy-IDs"]

uploaded_files = {}
cols = st.columns(3)

# File upload section
for i, name in enumerate(REQUIRED_FILES + OPTIONAL_FILES):
    with cols[i % 3]:
        required = name in REQUIRED_FILES
        file = st.file_uploader(
            f"Upload {'(Required) ' if required else '(Optional) '}`{name}.csv`", 
            type="csv", 
            key=name
        )
        if file:
            uploaded_files[name] = file

# Determine upload scenario
required_uploaded = all(f in uploaded_files for f in REQUIRED_FILES)
optional_uploaded = any(f in uploaded_files for f in OPTIONAL_FILES)
taxonomy_only = not required_uploaded and optional_uploaded

# Validate upload scenarios
if taxonomy_only:
    st.success("Taxonomy file uploaded. Ready to process.")
    # Process taxonomy file only
    taxonomy_df = pd.read_csv(StringIO(uploaded_files["new-taxonomy-IDs"].getvalue().decode("utf-8")))
    
    # Step 1: Remove duplicates within taxonomy file
    st.header("Taxonomy File Processing")
    sample_ids = taxonomy_df.iloc[:, 0].astype(str)
    duplicates = sample_ids[sample_ids.duplicated(keep='first')]
    
    if not duplicates.empty:
        st.warning(f"Found {len(duplicates)} duplicates in taxonomy file")
        with st.expander("View duplicates"):
            st.dataframe(taxonomy_df[taxonomy_df.iloc[:, 0].isin(duplicates)])
        
        if st.button("Remove duplicates from taxonomy file"):
            taxonomy_df = taxonomy_df.drop_duplicates(subset=taxonomy_df.columns[0], keep='first')
            st.success(f"Removed {len(duplicates)} duplicates")
            st.session_state.taxonomy_df = taxonomy_df
            st.rerun()
    else:
        st.success("No duplicates found in taxonomy file")
    
    if st.button("Upload Taxonomy to S3", 
                disabled=st.session_state.get('taxonomy_uploaded', False)):
        
        # Store the dataframe in session state if not already there
        if 'taxonomy_df' not in st.session_state:
            st.session_state.taxonomy_df = taxonomy_df
        
        file_dataframes = {"new-taxonomy-IDs": st.session_state.taxonomy_df}
        pre_upload_count = count_unique_records(file_dataframes)

        with st.spinner(f"Uploading {pre_upload_count} records..."):
            upload_to_s3(file_dataframes, [])
            st.session_state.taxonomy_uploaded = True  # Mark as uploaded
            st.success(f"Successfully uploaded {pre_upload_count} taxonomy records")
            st.session_state.clear()  # Clear session state after upload
            time.sleep(3)
            st.rerun()  # Refresh to show disabled button

elif required_uploaded:
    # Existing processing logic for required files (5 files)
    current_signatures = get_file_signatures(uploaded_files)
    
    if 'file_signatures' not in st.session_state:
        st.session_state.file_signatures = current_signatures
        st.session_state.step = 1
        st.session_state.file_dataframes = {}
        st.session_state.validation_passed = False
        st.success("All required files uploaded. Processing...")
    
    elif current_signatures != st.session_state.file_signatures:
        st.session_state.file_signatures = current_signatures
        st.session_state.step = 1
        st.session_state.file_dataframes = {}
        st.session_state.validation_passed = False
        st.rerun()

    # Initialize session state for tracking progress
    if 'step' not in st.session_state:
        st.session_state.step = 1
        st.session_state.file_dataframes = {}
        st.session_state.validation_passed = False
    
    # Load files if not already loaded
    if not st.session_state.file_dataframes:
        for label, file in uploaded_files.items():
            file.seek(0)
            df = pd.read_csv(StringIO(file.getvalue().decode("utf-8")))
            st.session_state.file_dataframes[label] = df
    
    # Step 1: Remove duplicates within each file (include taxonomy table)
    if st.session_state.step == 1:
        st.header("Step 1: Remove Duplicates Within Files")
        
        any_duplicates = False
        for label, df in st.session_state.file_dataframes.items():
            # Find duplicates
            sample_ids = df.iloc[:, 0].astype(str)
            duplicates = sample_ids[sample_ids.duplicated(keep='first')]
            
            if not duplicates.empty:
                any_duplicates = True
                st.warning(f"Found {len(duplicates)} duplicates in {label}")
                
                with st.expander(f"View duplicates in {label}"):
                    duplicate_records = df[df.iloc[:, 0].isin(duplicates)]
                    st.dataframe(duplicate_records)
                    
                    if st.button(f"Remove duplicates in {label}", key=f"remove_{label}"):
                        st.session_state.file_dataframes[label] = df.drop_duplicates(subset=df.columns[0], keep='first')
                        st.success(f"Removed {len(duplicates)} duplicates from {label}")
                        st.rerun()
        
        if not any_duplicates:
            st.success("No duplicates found within any files")
        
        if st.button("Proceed to Step 2", disabled=any_duplicates):
            st.session_state.step = 2
            st.rerun()
    
    # Step 2: Filter to keep only complete records (EXCLUDE taxonomy table)
    elif st.session_state.step == 2:
        st.header("Step 2: Check for Complete Records Across Files")
        
        # Get only required files for validation
        validation_files = {k:v for k,v in st.session_state.file_dataframes.items() 
                          if k in REQUIRED_FILES}
        
        # Find common sample IDs across required files
        all_sample_ids = []
        for label, df in validation_files.items():
            sample_ids = df.iloc[:, 0].apply(get_normalized_id)
            all_sample_ids.append(set(sample_ids))
        
        common_ids = set.intersection(*all_sample_ids)
        
        # Find missing IDs in each required file
        missing_info = {}
        for label, df in validation_files.items():
            sample_ids = set(df.iloc[:, 0].apply(get_normalized_id))
            missing_ids = sample_ids - common_ids
            if missing_ids:
                missing_info[label] = missing_ids
        
        if missing_info:
            st.warning("Some sample IDs are missing across required files")
            
            for label, missing_ids in missing_info.items():
                st.write(f"Missing IDs in {label}: {len(missing_ids)}")
                
                with st.expander(f"View records with missing IDs in {label}"):
                    df = validation_files[label]
                    missing_records = df[df.iloc[:, 0].apply(get_normalized_id).isin(missing_ids)]
                    st.dataframe(missing_records)
            
            if st.button("Remove records with missing IDs from required files"):
                for label in REQUIRED_FILES:
                    if label in st.session_state.file_dataframes:
                        df = st.session_state.file_dataframes[label]
                        sample_ids = df.iloc[:, 0].apply(get_normalized_id)
                        filtered_df = df[sample_ids.isin(common_ids)]
                        st.session_state.file_dataframes[label] = filtered_df
                
                st.session_state.step = 3
                st.rerun()
        else:
            st.success("All required files contain the same set of sample IDs")
            if st.button("Proceed to Step 3"):
                st.session_state.step = 3
                st.rerun()
    
    # Step 3: Check against data lake for duplicates (EXCLUDE taxonomy table)
    elif st.session_state.step == 3:
        st.header("Step 3: Check Against Existing Data")
        
        if "participant-metadata" in st.session_state.file_dataframes:
            metadata_df = st.session_state.file_dataframes["participant-metadata"]
            sample_ids = metadata_df.iloc[:, 0].apply(get_normalized_id)
            cross_duplicates = check_dynamodb_for_duplicates(sample_ids.unique())
            
            if cross_duplicates:
                st.error(f"Found {len(cross_duplicates)} duplicates against existing data")
                
                with st.expander("View duplicate records"):
                    duplicate_records = metadata_df[sample_ids.isin(cross_duplicates)]
                    st.dataframe(duplicate_records)
                    
                    if st.button("Remove all duplicate entries from required files"):
                        for label in REQUIRED_FILES:
                            if label in st.session_state.file_dataframes:
                                df = st.session_state.file_dataframes[label]
                                file_sample_ids = df.iloc[:, 0].apply(get_normalized_id)
                                clean_df = df[~file_sample_ids.isin(cross_duplicates)]
                                st.session_state.file_dataframes[label] = clean_df
                        
                        st.session_state.validation_passed = True
                        st.rerun()
            else:
                st.success("No duplicates found against existing data")
                st.session_state.validation_passed = True
    
    # Modify the final validation section
    if st.session_state.validation_passed:
        # Check only required files for emptiness
        empty_files = [label for label in REQUIRED_FILES 
                    if label in st.session_state.file_dataframes and 
                    len(st.session_state.file_dataframes[label]) == 0]
        
        if empty_files:
            st.error(f"Error: Required files have no valid records: {', '.join(empty_files)}")
            st.error("Cannot upload - required files are empty")
        else:
            # Calculate and show record counts before upload
            total_records = count_unique_records(st.session_state.file_dataframes)
            required_records = count_unique_records(st.session_state.file_dataframes, required_only=True)
            
            st.success("All validations passed. Ready to upload to S3.")
            st.info(f"Total complete records to upload: {total_records}")
            st.info(f"Complete records in required files: {required_records}")
            st.info(f"Records in optional file(s): {total_records - required_records}")
            
            if 'upload_complete' not in st.session_state:
                st.session_state.upload_complete = False
                
            if st.button("Upload to S3", disabled=st.session_state.upload_complete):
                # Store counts before upload
                pre_upload_count = count_unique_records(st.session_state.file_dataframes)
                
                # Upload files
                metadata_df = st.session_state.file_dataframes["participant-metadata"]
                sample_ids = metadata_df.iloc[:, 0].apply(get_normalized_id)
                metadata_normalized_ids = sample_ids.unique().tolist()
                
                with st.spinner(f"Uploading {pre_upload_count} records..."):
                    upload_to_s3(st.session_state.file_dataframes, metadata_normalized_ids)
                    
                    # Show post-upload confirmation
                    st.success(f"Successfully uploaded {pre_upload_count} unique records to S3")
                    st.session_state.upload_complete = True
                    st.session_state.clear()
                    time.sleep(3)
                    st.rerun()

else:
    st.info("Please upload all 5 required files.")