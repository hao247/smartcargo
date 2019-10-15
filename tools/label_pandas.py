import psycopg2 as pg
import yaml


"""
port labeling function by using pandas cut
similar idea as to bucketizer but with no parallel processing
slower than bucketizer method, only for benchmarking
"""


def label_ports(rang):
    """
    labels trips with starting and ending port names
    automatically reads dataframe from psql table
    and saves to psql after processing
    :type rang:     float   size/range of the port in degrees
    """
    credent = yaml.load(open("../config/credentials.yaml", "r"))
    conn = pg.connect(
        host=credent["psql"]["host"],
        database=credent["psql"]["dbname"],
        user=credent["psql"]["user"],
        password=credent["psql"]["passwd"],
    )
    cursor = conn.cursor()
    trips = pd.read_sql_query("select * from trips", con=conn)
    ports = pd.read_csv("s3://hao-zheng-databucket/ports/Major_Ports.csv")
    trips["departure"] = ""
    trips["arrival"] = ""

    for index, port in ports.iterrows():
        lat = port["Y"]
        lon = port["X"]
        lat_bin = [-90, lat - rang, lat + rang, 90]
        lon_bin = [-180, lon - rang, lon + rang, 180]
        scores = [0, 4, 2]
        score_cols = [
            "lat_start_score",
            "lon_start_score",
            "lat_end_score",
            "lon_end_score",
        ]
        coord_cols = ["lat_start", "lon_start", "lat_end", "lon_end"]
        bins = [lat_bin, lon_bin, lat_bin, lon_bin]
        for i in len(score_cols):
            trips[score_cols[i]] = pd.cut(
                trips[coord_cols[i]], bins[i], labels=scores
            ).astype(int)

        trips["start_score"] = trips["lat_start_score"] + trips["lon_start_score"]
        trips["end_score"] = trips["lat_end_score"] + trips["lon_end_score"]

        total_bin = [-1, 7, 10]
        total_scores = ["", port["PORT_NAME"]]

        trips["departure"] = pd.cut(
            trips["start_score"], total_bin, labels=total_scores
        ).astype(str)
        trips["arrival"] = pd.cut(
            trips["end_score"], total_bin, labels=total_scores
        ).astype(str)
        trips["departure"] = trips["departure"].str.cat(trips["departure"])
        trips["arrival"] = trips["arrival"].str.cat(trips["arrival"])

    scores = [
        "lat_start_score",
        "lon_start_score",
        "lat_end_score",
        "lon_end_score",
        "departure_tmp",
        "arrival_tmp",
        "start_score",
        "end_score",
    ]
    trips.drop(scores, axis=1)
