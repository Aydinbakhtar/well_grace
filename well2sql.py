"""This code tries to store wells data in sql
    tables to get better performance in next steps"""

try:
    import geopandas as gpd
    import psycopg2
    import pandas as pd
    import pyproj
    from sqlalchemy import create_engine
    from shapely.geometry import Point
    from geoalchemy2.elements import WKBElement
    from persiantools.jdatetime import JalaliDate
    import datetime
    import warnings
    warnings.filterwarnings('ignore')
except ImportError as e:
    print("Error: ", e)
    print("Please install the missing libraries")
    # raise SystemExit



# connect to my db
try:
    conn_shp = psycopg2.connect(
        host="localhost",
        database="iran_shp",
        user="postgres",
        password="xcxck"
    )
    conn_well = psycopg2.connect(
        host="localhost",
        database="grace",
        user="postgres",
        password="xcxck"
    )

    conn_ir_well = psycopg2.connect(
        host="localhost",
        database="iran_well",
        user="postgres",
        password="xcxck"
    )

except ImportError as e:
    print("Error: ", e)
    print("Please Check Postgres Connctions !")    



try:
    cur = conn_well.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    rows = cur.fetchall()
    province_list = [row[0] for row in rows[3:]]
    print('Cursor Over the well data')

except ImportError as e:
    print("Error: ", e)
    print("Please Check Postgres Connctions !")   


province_dic = {}

for province in province_list:
    print(province)
    well_query = "SELECT * FROM {}".format(province)
    province_dic[province] = gpd.read_postgis(well_query, conn_well, geom_col='geom')



print('Implementing Classes Started ...')

class IdGenerator:
    def __init__(self, name, data_frame):
        self.name = name
        self.data_frame = data_frame
        

    def abb_db(self):
        
        full_name = self.name[:-3]
        utm_zone = self.name[-2:]

        cur = conn_well.cursor()
        query = "SELECT abbreviation FROM output.provinces WHERE table_name = %s"
        cur.execute(query, (full_name, ))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No abbreviation found for table name {full_name}")
        global well_id_head
        well_id_head = str(row[0]) + '-' + str(utm_zone)
        return well_id_head

    
    def unique_digits(self, factorize_column):
        tmp_list = pd.factorize(self.data_frame[factorize_column])[0] + 1
        tmp_list = [f'{x:03}' for x in tmp_list]
        self.data_frame['well_id'] = tmp_list
        well_id = well_id_head + '-' + self.data_frame['well_id'].astype(str)
        self.data_frame['well_id'] = well_id
        return self.data_frame

def jalali_to_gregorian(year, month, day):
    j_date = JalaliDate(year, month, day).to_gregorian()
    g_year = j_date.year
    g_month = j_date.month
    g_day = j_date.day
    return pd.Timestamp(year=g_year, month=g_month, day=g_day).date()

ign_list = [
    'geom',
    'sazeman',
    'ostan',
    'sal1',
    'mah',
    'rooz',
    'code',
    'mahdoodeh',
    'mahal',
    'ellat-adam',
    'molahezat'
    ]


new_col_list = ['well_id', 'utmx', 'utmy','taraz', 'sath-ab', 'date']



class df_Trimmer():
    def __init__(self, data_frame, ign_list = ign_list, new_col_list = new_col_list):
        self.data_frame = data_frame
        self.ign_list = ign_list
        self.new_col_list = new_col_list

    def date_conv(self, new_date_column, year_column, month_column, day_column):
        self.data_frame[new_date_column] = self.data_frame.apply(lambda row: jalali_to_gregorian(row[year_column], row[month_column], row[day_column]), axis=1)
        self.data_frame = self.data_frame.drop(columns=self.ign_list)
        self.data_frame = self.data_frame.reindex(columns=self.new_col_list)
        df_loc = self.data_frame.groupby('well_id').agg({'utmx': 'first', 'utmy': 'first'}).reset_index()
        return self.data_frame, df_loc


# make a class to reproject projections

class Projection:
    def __init__(self, data_frame, utm_zone):
        self.utm_zone = utm_zone
        self.data_frame = data_frame

    def reproj(self):
        utm_proj = pyproj.Proj(proj='utm', zone=self.utm_zone, ellps='WGS84')
        wgs84_proj = pyproj.Proj(proj='latlong', datum='WGS84')

        for idx, row in self.data_frame.iterrows():
            utm_point = Point(row['utmx'], row['utmy'])
            lon, lat = pyproj.transform(utm_proj, wgs84_proj, utm_point.x, utm_point.y)

            # wgs84_point = Point(lon, lat)
            self.data_frame.loc[idx, 'lon'] = lon
            self.data_frame.loc[idx, 'lat'] = lat
        
        return self.data_frame
    
print('Implementing Classes Finished ...')
    


for key in province_dic.keys():
    tmp_df = province_dic[key]
    province_abb = IdGenerator(key, tmp_df)
    a = province_abb.abb_db()
    tmp_df_abb = province_abb.unique_digits('mahal')
    tmp_trim = df_Trimmer(tmp_df_abb)
    print(f"Triminig, {key}, Data Frame".format(key))
    df, loc_df = tmp_trim.date_conv('date' ,'sal1', 'mah', 'rooz')
    proj_zanjan_39 = Projection(loc_df, 39)
    print(f"Projecting, {key}, Data Frame".format(key))
    c_df = proj_zanjan_39.reproj()
    engine = create_engine('postgresql://postgres:xcxck@localhost:5432/iran_well')
    print(f"Make, {key}, Table".format(key))
    df.to_sql('well_data', engine, if_exists='append', index=False)
    print(f"Make, {key}, Locatio df".format(key))
    c_df.to_sql('well_loc', engine, if_exists='append', index=False)





