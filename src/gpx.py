from datetime import datetime, timedelta
import pandas as pd
import xml.etree.ElementTree as ET
from geopy.distance import geodesic


def parse_gpx(xml_text):
    # Parse the XML text
    root = ET.fromstring(xml_text)
    
    # Define the namespaces
    namespaces = {
        'default': 'http://www.topografix.com/GPX/1/1'
    }
    
    # Extract waypoint data
    waypoints = []
    for wpt in root.findall('default:wpt', namespaces):
        name = wpt.find('default:name', namespaces).text
        lon = wpt.attrib['lon']
        lat = wpt.attrib['lat']
        waypoints.append({'name': name, 'lon': lon, 'lat': lat})
    
    # Create DataFrame
    df_wpt = pd.DataFrame(waypoints)

    # extract track data
    trackpoints = []
    for trkpt in root.findall('default:trk/default:trkseg/default:trkpt', namespaces):
        lon = trkpt.attrib['lon']
        lat = trkpt.attrib['lat']
        trackpoints.append({'lon': lon, 'lat': lat})

    # Create DataFrame
    df_trk = pd.DataFrame(trackpoints)

    return df_wpt, df_trk


def calculate_elapsed_time(df, average_riding_speed_mps, start_time=datetime.now()):
    # Compute distances between waypoints
    distances = []
    total_distance = [0]
    total_distance_m = 0
    for i in range(1, len(df)):
        coords_1 = (df.loc[i-1, 'lat'], df.loc[i-1, 'lon'])
        coords_2 = (df.loc[i, 'lat'], df.loc[i, 'lon'])
        distance = geodesic(coords_1, coords_2).meters
        distances.append(distance)
        total_distance_m += distance
        total_distance += [total_distance_m]

    df["distance"] = [0] + distances
    
    # Compute elapsed times
    elapsed_times_seconds = [distance / average_riding_speed_mps for distance in distances]
    elapsed_times_minutes = [time / 60 for time in elapsed_times_seconds]
    
    # Create cumulative elapsed time
    cumulative_elapsed_time = [0] + list(pd.Series(elapsed_times_minutes).cumsum())
    
    # Add elapsed time to DataFrame
    df['elapsed_time_minutes'] = cumulative_elapsed_time

    dt = [start_time + timedelta(minutes=elapsed_time) for elapsed_time in cumulative_elapsed_time]
    df['time'] = dt
    df['total_distance'] = total_distance
    print(f"start_time: {start_time} elapsed_time: {cumulative_elapsed_time} tile: {dt} total distance: {total_distance} meters")
    
    return df


def load_gpx(fname):
    with open(fname, "r") as f:
        contents = f.read()

    print(f"reading {contents}")

    df_wpt, df_trk = parse_gpx(contents)
    return df_wpt, df_trk
