

def upload_duck(dataset, datasetter_config):
    db_config = datasetter_config["database"]
    constr = connection_code_from_config(datasetter_config)
    path = datasetter_config["datasets"][dataset]["path"]
    # TODO: currently there is nothing handling the exceptional cases, these should be handled ahead of time, defaults should be set for empty datasets configurations like raw for dataset type should be set for datasets that are not 'collected'.
    data_type = datasetter_config["datasets"][dataset]["type"]
    if data_type == "raw":
        typer.echo(err=True, message="Raw datasets cannot be read by duckdb, please convert to a viable duck method")
    with duckdb.connect(constr) as conn:
        finsert = "URL"
        query = f"""
            INSERT INTO OR IGNORE IF EXISTS {table}
            ({dataset}, {data_type}, ?, NULL, ?, current_localtimestamp());
            """
        
        if data_type not in ["blob", "image", "largeText", "document"]:
             query = f"""
                INSERT INTO OR IGNORE IF EXISTS {table}
                ({dataset}, {data_type}, NULL, ?, ?, current_localtimestamp());
                """
             insertion_type = "SmallText"
        insertions = [[x, y] for (x, y) in conn.execute(f"CREATE TEMP TABLE t AS SELECT {insertion_type}, details FROM '{path}'").fetchall()]
        conn.execute(query, insertions)
    return [(x, db_config["location"]) for x in insertions]


def upload_blobs(bucket_name, dataset_files, source_dir, workers=8):
    """Uploads dataset files to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    uploads = []
    if len(dataset_files) <= 1:
        return upload_blob(bucket_name, dataset_files[0], source_dir)
    filenames = dataset_files
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    results = storage.transfer_manager.upload_many_from_filenames(
    bucket, filenames, source_directory=source_dir, max_workers=workers
    )
    
    for name, result in zip(filenames, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.
        if isinstance(result, Exception):
            typer.echo(f"Failed to upload {name} due to exception: {result}", err=True)
        else:
            uploads.append((name, bucket.name))
    return uploads

def upload_blob(bucket_name, dataset_file, destination_blob_name):
     """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(dataset_file, if_generation_match=generation_match_precondition)
    return [(dataset_file, bucket.name)]
