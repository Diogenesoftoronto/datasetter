import typer
from typing import Mapping, Optional, Sequence
from typing_extensions import Annotated
import src.config as conf
import src.scraper as scraper
from pathlib import Path
from enum import Enum, auto
import toml
from dotenv import load_dotenv as load_env
from os import getenv
from google.cloud import storage
import duckdb

app = typer.Typer()


class Source(Enum):
    instagram = auto()
    pinterest = auto()
    reddit = auto()
    kaggle = auto()
    huggingface = auto()


class Location(Enum):
    motherduck = auto()
    cloudflare = auto()
    gcp = auto()
    kaggle = auto()


@app.command()
def upload(
    dataset: str,
    location: Location,
    config: Annotated[Optional[Path]] = Path("config.toml"),
):
    """This function uploads dataset to gcloud, and motherduck or another database for further processing"""
    with config.open() as f:
        datasetter_config = toml.load(f)
        location = (
            location
            if datasetter_config["database"]["location"] is not location.name
            and Location.motherduck is not location
            else datasetter_config["database"]["location"]
        )
    filenames, src_dir = get_info_from_dataset(dataset)
    match location.name:
        case Location.gcp.name:
            if datasetter_config["datasets"][dataset]["type"] in ["image", "blob"]: 
                uploads = upload_blob(datasetter_config["database"]["bucket"], filenames, src_dir)
            uploads = upload_gcp
        case Location.motherduck.name:
            # TODO: should change the cases to also indicate the progress state with uploading. Tell the user that it is uploading to location.
            uploads = upload_duck(dataset, datasetter_config)
        case Location.cloudflare.name:
            typer.echo("cloudflare is not a viable location to upload yet.",err=True)
            typer.abort()
        case _:
            typer.echo("This shall never be supported", err=True)
            typer.abort()
    [typer.echo(f"Uploaded: {item}, to {place}") for (item, place) in uploads]
    return


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
        con.execute(query, insertions)
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
    if len(dataset_files) > 1:
        uploads = upload_blob(bucket_name, dataset_files[0], source_dir)
        
    filenames = dataset_files
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    results = transfer_manager.upload_many_from_filenames(
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


@app.command()
def collect(
    data: Annotated[Path],
):
    """Collect data from a variety of locations and create a dataset. A dataset can be a csv, parquet file or anything that is readable via duckdb or a collection of blobs. You need to provide information as to the type of dataset it is, where the data was sourced, e.g. Pinterest or Instagram, as well as an optional retension period, you may also add a description for the dataset as well as any additional dataset metadata in the metadata.json file."""
    pass
@app.command()
def fetch(method):
    """Fetch data using a variety of methods for further processing before being collected into a dataset. This function was not combined with collect because there may be instances where you have the same underlying data but may want to structure the metadata differently. Fetching allows one to choose from various locations whether that be kaggle, huggingface, or pinterest. All of these are great places to fetch data from for one's purposes. 
    """
@app.command()
def list(
    dirs: Annotated[Sequence[Path]],
    out: Annotated[Optional[Path]],
    config: Annotated[Path] = Path("config.toml"),
    search: Annotated[Optional[str]] = None,
):
    """Return a list of the datasets that we have and metadata about them"""
    with config.open() as f:
        datasetter_config = toml.load(f)
        datasets = datasetter_config["datasets"].keys()
    datasetter_datasets = []
    for d in dirs.iterdir():
        if d.name in datasets:
            # skip over datasets in our configuration
            continue
        elif not d.is_dir():
            typer.echo(
                f"WARNING: {d} is not a directory, datasets are always directories. This cannot be listed.",
                err=True,
            )
            continue
        # write to optional output file
        message = f"[{d}] dataset, {config['datasets'][d.name]}"
        datasetter_datasets.append(message)
        typer.echo(message, file=out)
    else:
        typer.echo(f"All datasets returned. {len(datasetter_datasets)}")


def instagram(
    search: str,
    amount: int = 1,
    download: bool = False,
    config: Annotated[Path] = Path("config.toml"),
):
    pass


def connection_code_from_config(config):
    database_conf = config["database"]
    # add optional checks
    load_env()
    location = database_conf["location"]
    name = database_conf["name"]
    if location in ["motherduck", "md"] and databaseType == "duckdb":
        database = location + ":" + name
        return f"{database}?motherducktoken={getenv('motherduck_token')}"
    else:
        typer.echo("WARNING: no connection code for database.", err=True)
        typer.abort()


def roma(amount: int = 1, config: Annotated[Path] = Path("config.toml")):
    with config.open() as f:
       datasetter_config = toml.load(f)
    constr = connection_code_from_config(datasetter_config)
    # register gcloud functions with duckdb so that I can query them
    # store image links in the database
    # use CLIP or Mobile clip to generate image embeddings, add them to the database, then combine them with the other models made by openai we can experiment to see if open
    with duckdb.connect(constr) as conn:
        query = f"""SELECT gvision( || ProductDescription ) FROM Parts WHERE PartNum NOT LIKE 'M-%' LIMIT {amount}"""  # where are not going to be using matts here.
        pass


def pinterest(
    search: str,
    amount: int = 10,
    quality: str = "orig",
    bookmarks: str = "",
    download: bool = False,
    config: Annotated[Optional[Path]] = "config.toml",
):
    with config.open() as f:
        datasetter_config = toml.load(f)
    download = download if download else config["download"]
    table = datasetter_config["table"]
    pinterest_config = datasetter_config.PinterestConfig(
        search_keywords=search,  # Search word
        file_lengths=amount,  # total number of images to download (default = "100")
        image_quality=quality,  # image quality (default = "orig")
        bookmarks="",
    )  # next page data (default= "")
    images, urls = scraper.PinterestScraper(
        pinterest_config
    ).return_images()  # download images directly
    if download:
        scraper.PinterestScraper(configs).download_images()
    if sql:
        with duckdb.connect(constr) as conn:
            query = f"""
            INSERT INTO OR IGNORE IF EXISTS {table}
            ("pinterest", "image", ?, ?, current_localtimestamp());
            """
            insertions = [
                url
            ]  # TODO: finish writing the list comprehension for the necessary insertions in the database
            con.execute(query, *insertions)
    for image, url in zip(images, urls):
        typer.echo(url)  # just bring image links
