from argparse import ArgumentParser
from datetime import datetime
import enum
import json
from pathlib import Path
import re
from subprocess import run
import sqlalchemy as db
from sqlalchemy import orm, sql

BaseSqlite = orm.declarative_base()
BaseSql = orm.declarative_base()


# https://docs.sqlalchemy.org/en/14/tutorial/metadata.html#declaring-mapped-classes
class GeosPathsSqlite(BaseSqlite):
    __tablename__ = 'GeosPaths'

    path_id = db.Column(db.Integer, primary_key=True)
    root_path = db.Column(db.Text)
    product = db.Column(db.Text)
    levels = db.Column(db.Text)
    type = db.Column(db.Text)

    def __repr__(self) -> str:
        return f'GeosPathsSqlite(path_id = {self.path_id}, root_path = {self.root_path}, product = {self.product}, levels = {self.levels}, type = {self.type})'


class GeosPathsSql(BaseSql):
    __tablename__ = 'GeosPaths'

    path_id = db.Column(db.Integer, primary_key=True)
    root_path = db.Column(db.Text)
    product = db.Column(db.String(8))
    levels = db.Column(db.String(8))
    data_type = db.Column(db.String(8))

    def __repr__(self) -> str:
        return f'GeosPathsSqlite(path_id = {self.path_id}, root_path = {self.root_path}, product = {self.product}, levels = {self.levels}, type = {self.data_type})'

    @classmethod
    def from_sqlite(cls, obj):
        return cls(path_id=obj.path_id, root_path=obj.root_path, product=obj.product, levels=obj.levels, data_type=obj.type)


class GeosFilesSqlite(BaseSqlite):
    __tablename__ = 'Geos'

    file_id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.Text)
    product = db.Column(db.String(8))
    filedate = db.Column(db.DateTime)
    levels = db.Column(db.String(8))
    type = db.Column(db.String(8))

    def __repr__(self) -> str:
        return f'GeosFilesSqlite(file_id = {self.file_id}, path = {self.path}, product = {self.product}, filedate = {self.filedate}, levels = {self.levels}, type = {self.type})'


class MetFilesSql(BaseSql):
    __tablename__ = 'MetFiles'

    file_id = db.Column(db.Integer, primary_key=True)
    file_path = db.Column(db.Text)
    product = db.Column(db.String(8))
    filedate = db.Column(db.DATETIME)
    levels = db.Column(db.String(8))
    data_type = db.Column(db.String(8))

    def __repr__(self) -> str:
        return f'MetFilesSql(file_id = {self.file_id}, file_path = {self.file_path}, product = {self.product}, filedate = {self.filedate}, levels = {self.levels}, type = {self.data_type})'

    @classmethod
    def from_sqlite(cls, obj, unknown_products=set()):
        if obj.product == 'fpit':
            product = 'geosfpit'
        elif product not in unknown_products:
            product = obj.product
            unknown_products.add(product)
            print(f'WARNING: unknown product {product}, double check that it is recognized by AutoModRust')
        else:
            product = obj.product

        return cls(file_id=obj.file_id, file_path=obj.path, product=product, filedate=obj.filedate, levels=obj.levels, data_type=obj.type)


_map_type_mapping = {'none': 'None', 'text': 'Text', 'netcdf': 'NetCDF'}


class JobsSqlite(BaseSqlite):
    __tablename__ = 'Jobs'

    job_id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.Integer)
    site_id = db.Column(db.Text)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    lat = db.Column(db.Text)
    lon = db.Column(db.Text)
    email = db.Column(db.Text)
    delete_time = db.Column(db.DateTime)
    priority = db.Column(db.Integer)
    save_dir = db.Column(db.Text)
    save_tarball = db.Column(db.Integer)
    mod_fmt = db.Column(db.Text)
    map_fmt = db.Column(db.Text)
    vmr_fmt = db.Column(db.Text)
    submit_time = db.Column(db.DateTime)
    complete_time = db.Column(db.DateTime)
    output_file = db.Column(db.Text)


class JobsSql(BaseSql):
    __tablename__ = 'Jobs'

    job_id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.Integer)
    site_id = db.Column(db.JSON)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    lat = db.Column(db.JSON)
    lon = db.Column(db.JSON)
    email = db.Column(db.String(64))
    delete_time = db.Column(db.DateTime)
    priority = db.Column(db.Integer)
    queue = db.Column(db.String(32))
    save_dir = db.Column(db.Text)
    save_tarball = db.Column(db.Integer)
    mod_fmt = db.Column(db.String(8))
    map_fmt = db.Column(db.String(8))
    vmr_fmt = db.Column(db.String(8))
    submit_time = db.Column(db.DateTime)
    complete_time = db.Column(db.DateTime)
    output_file = db.Column(db.Text)

    @classmethod
    def from_sqlite(cls, obj: JobsSqlite):
        site_id = obj.site_id.split(',')
        lat = cls._convert_latlon(obj.lat)
        lon = cls._convert_latlon(obj.lon)
        site_id, lat, lon = cls._match_lengths(site_id, lat, lon)

        mod_fmt = obj.mod_fmt.capitalize()
        vmr_fmt = obj.vmr_fmt.capitalize()
        map_fmt = _map_type_mapping[obj.map_fmt]

        if obj.email is None and all(x is None for x in lon):
            queue = 'std-sites'
        else:
            queue = 'submitted'

        if obj.delete_time == datetime(9999, 12, 31, 23, 59, 59):
            delete_time = None
        else:
            delete_time = obj.delete_time

        # met_key and ginput_key are both NULL in the test so far (meaning use the default met/ginput for those dates)
        # so we should not need to set them, in theory.

        return cls(job_id=obj.job_id, state=obj.state, site_id=site_id, start_date=obj.start_date, end_date=obj.end_date,
                   lat=lat, lon=lon, email=obj.email, delete_time=delete_time, priority=obj.priority, queue=queue, save_dir=obj.save_dir,
                   save_tarball=obj.save_tarball, mod_fmt=mod_fmt, map_fmt=map_fmt, vmr_fmt=vmr_fmt, submit_time=obj.submit_time,
                   complete_time=obj.complete_time, output_file=obj.output_file)

    @staticmethod
    def _convert_latlon(coords):
        coords = [float(x) for x in coords.split(',')]
        if len(coords) == 1 and coords[0] < -999:
            return [None]
        else:
            return coords

    @staticmethod
    def _match_lengths(*args):
        if all(len(x) == 1 for x in args):
            return args
        n = set(len(x) for x in args if len(x) != 1)

        if len(n) != 1:
            raise ValueError('Input arguments are not all length 1 or n')
        n = n.pop()
        out = []
        for a in args:
            if len(a) == 1:
                out.append(a * n)
            else:
                out.append(a)
        return out


class StdSiteSqlite(BaseSqlite):
    __tablename__ = 'StdSites'

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    job_id = db.Column(db.Integer)
    day_state = db.Column(db.Integer)


class SiteTypeEnum(enum.Enum):
    Unknown = 1
    TCCON = 2
    EM27 = 3


class StdOutputStructure(enum.Enum):
    FlatModVmr = 1
    FlatAll = 2
    FlatAllMapNc = 3
    TreeModVmr = 4
    TreeAll = 5
    TreeAllMapNc = 6



class StdSiteSql(BaseSql):
    __tablename__ = 'StdSiteList'

    id = db.Column(db.Integer, primary_key=True)
    site_id = db.Column(db.String(2))
    name = db.Column(db.String(32))
    site_type = db.Column(db.Enum(SiteTypeEnum))
    output_structure = db.Column(db.Enum(StdOutputStructure))


class StdSiteJob(BaseSql):
    __tablename__ = 'StdSiteJobs'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    site = db.Column(db.Integer)
    date = db.Column(db.Date)
    state = db.Column(db.Integer)
    job = db.Column(db.Integer)
    tarfile = db.Column(db.Text)

    def __repr__(self) -> str:
        return f'StdSiteJob({self.site} {self.date}: job {self.job}, state {self.state}'


class StdSiteInfoSql(BaseSql):
    __tablename__ = 'StdSiteInfo'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    site = db.Column(db.Integer)
    location = db.Column(db.String(64))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    comment = db.Column(db.Text)


def migrate(sites_json, sqlite_db, sql_db=None, sql_user=None, sql_pw=None, host='localhost', dotenv_file=None, reset_database=False):
    if dotenv_file is not None:
        mysql_url = _get_mysql_dotenv_url(dotenv_file)
    elif sql_db is None or sql_user is None or sql_pw is None:
        raise TypeError('dotenv_file or all of sql_db, sql_user, and sql_pw are required')
    else:
        mysql_url = f'mysql://{sql_user}:{sql_pw}@{host}/{sql_db}?charset=utf8mb4'

    if reset_database:
        # cargo doesn't get the URL with the charset thing added normally
        _reset_db_with_cargo(mysql_url.split('?', maxsplit=1)[0])

    print('Copying')
    print(f'sqlite:///{sqlite_db}')
    print('to')
    print(_obscure_pw(mysql_url))
    # The utf8mb4 is required to handle all unicode characters, see https://docs.sqlalchemy.org/en/14/dialects/mysql.html#charset-selection
    sqlite_engine = db.create_engine(f'sqlite:///{sqlite_db}', future=True)
    mysql_engine = db.create_engine(mysql_url, future=True)
    add_site_ids(mysql_engine, sites_json)
    add_site_info(mysql_engine, sites_json)
    migrate_table(sqlite_engine, mysql_engine, GeosPathsSqlite, GeosPathsSql)
    migrate_table(sqlite_engine, mysql_engine, GeosFilesSqlite, MetFilesSql)
    migrate_table(sqlite_engine, mysql_engine, JobsSqlite, JobsSql)
    migrate_std_sites(sqlite_engine, mysql_engine)

def _get_mysql_dotenv_url(dotenv_file):
    with open(dotenv_file) as f:
        for line in f:
            if line.startswith('DATABASE_URL'):
                url = line.split('=')[1].strip().strip('"')
                return f'{url}?charset=utf8mb4'
            
    raise IOError('Missing DATABASE_URL in dotenv file')


def _obscure_pw(mysql_url):
    return re.sub(r':\w+@', ':********@', mysql_url)


def _reset_db_with_cargo(db_url):
    print(f'Resetting SQL database at {_obscure_pw(db_url)}')
    working_dir = Path(__file__).absolute().parent.parent.as_posix()
    run(['cargo', 'sqlx', 'database', 'drop', '-y', '--database-url', db_url], cwd=working_dir)
    run(['cargo', 'sqlx', 'database', 'create', '--database-url', db_url], cwd=working_dir)
    run(['cargo', 'sqlx', 'migrate', 'run', '--database-url', db_url, '--source', './core-orm/migrations'], cwd=working_dir)


def migrate_table(sqlite_engine, mysql_engine, sqlite_cls, sql_cls):
    stmt = db.select(sqlite_cls)
    with orm.Session(sqlite_engine) as s_lite, orm.Session(mysql_engine) as s_mysql:
        # Clear existing information
        s_mysql.execute(db.delete(sql_cls))
        
        # Copy from sqlite to mysql
        for i, row in enumerate(s_lite.execute(stmt), start=1):
            print(f'\rCopying row {i} from {sqlite_cls.__tablename__}', end='')
            o = sql_cls.from_sqlite(row[0])
            s_mysql.add(o)
            
        s_mysql.commit()
        print('\nDone.')


def migrate_std_sites(sqlite_engine, mysql_engine):
    # import pdb; pdb.set_trace()
    # stmt = db.select(StdSiteSqlite)
    with sqlite_engine.connect() as conn:
        meta = db.MetaData(conn)
        table = db.Table(StdSiteSqlite.__tablename__, meta, autoload=True)
        sites = sorted([m.key for m in table.columns if len(m.key) == 2 and m.key not in StdSiteSqlite.__table__.columns.keys()])
    
    site_mapping = dict()

    with orm.Session(sqlite_engine) as s_lite, orm.Session(mysql_engine) as s_mysql:
        s_mysql.execute(db.delete(StdSiteJob))
        for site in sites:
            stmt = db.select(StdSiteSql).where(StdSiteSql.site_id == site)
            result = s_mysql.execute(stmt).scalar()
            site_mapping[site] = result.id

        # My sqlite3 StdSites table has some duplicate rows. I'm sure we could come up with clever
        # SQL to select the entire rows with the most recent job, but getting the ORM to return
        # arbitrary columns is difficult.
        #
        # There will be some inaccuracy in job IDs, since if a day had some sites filled in later,
        # the row's job ID will only be for the latest run for that day.
        #
        # If a day isn't completed in the old sqlite table, then we probably shouldn't copy it and just
        # let the Rust automation catch up eventually. Also, in my test case, the only day states were 0 and 2,
        # and the 0s looked like backfilled days that got missed somehow.
        result = s_lite.execute(sql.text('SELECT * FROM StdSites WHERE day_state != 0'))
        dates = set()
        for i, row in enumerate(result, start=1):
            # Really this shouldn't trigger any more, selecting only rows with day_state != 0 should solve this.
            if row['date'] in dates:
                raise NotImplementedError('Duplicate date in std sites table')
        

            print(f'\rCopying row {i} from StdSites', end='')
            dates.add(row['date'])
            date_obj = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
            site_states = []
            day_state = row['day_state']
            for site in sites:
                site_foreign_key = site_mapping[site]
                site_state = row[site]
                state, do_add = _map_old_std_site_states_to_new(day_state, site_state)
                if do_add:
                    job_id = row['job_id'] if state >= 0 else None
                    # state == 2 means complete in the new scheme, that's the only state that should have an output tarfile
                    assumed_tarfile = f'/oco2-data/tccon/ftp/ginput-std-sites/tarballs/{site}/{site}_ggg_inputs_{date_obj:%Y%m%d}.tgz' if state == 2 else None
                    site_states.append(StdSiteJob(site=site_foreign_key, date=row['date'], job=job_id, state=state, tarfile=assumed_tarfile))
            s_mysql.add_all(site_states)
            s_mysql.commit()
        print('\nDone.')


def _map_old_std_site_states_to_new(day_state, site_state):
    if day_state != 2:
        raise NotImplementedError('day_state != 2')
    
    if site_state == 0:
        return 0, True  # old pending assumed to be the same as new JobNeeded
    elif site_state == 1:
        return 2, True  # old complete assumed to be the new Complete
    elif site_state == -1:
        return -3, False  # old nonop, don't add to the table
    else:
        raise NotImplementedError(f'site_state = {site_state}')


def add_site_ids(sql_engine, site_json):
    with open(site_json) as f:
        site_info = json.load(f)

    # Collapse this into a dictionary so that we have one entry per site - 
    # assume that the name will not change over time, since the old ginput
    # dictionary didn't allow for that.
    site_info = {i['site_id']: i for i in site_info}

    with orm.Session(sql_engine) as s_mysql:
        for sid, info in site_info.items():
            # The old ginput sites should be all TCCON, so they should all use the flat mod & vmr only output structure
            new_site = StdSiteSql(site_id=sid, name=info['name'], site_type=SiteTypeEnum.TCCON, output_structure=StdOutputStructure.FlatModVmr)
            s_mysql.add(new_site)

        s_mysql.commit()


def add_site_info(sql_engine, site_json):
    with open(site_json) as f:
        site_info = json.load(f)

    with orm.Session(sql_engine) as s_mysql:
        for info in site_info:
            stmt = db.select(StdSiteSql).where(StdSiteSql.site_id == info['site_id'])
            result = s_mysql.execute(stmt).scalar()
            new_info = StdSiteInfoSql(
                site=result.id,
                location=info['location'],
                latitude=info['latitude'],
                longitude=info['longitude'],
                start_date = datetime.strptime(info['start_date'], '%Y-%m-%d').date(),
                end_date = None if info['end_date'] is None else datetime.strptime(info['end_date'], '%Y-%m-%d').date(),
                comment=info.get('comment', '')
            )
            s_mysql.add(new_info)
        s_mysql.commit()


def main():
    p = ArgumentParser('Migrate an AutoModPython sqlite3 database to MySQL')
    p.add_argument('--host', default='localhost', help='The host that the MySQL database resides on. Default is %(default)s.')
    p.add_argument('--reset-database', action='store_true', help='Call cargo sqlx to reset the database to a clean state.')
    p.add_argument('--dotenv-file', help='If given, read the MySQL URL from the DATABASE_URL line in this file. This removes the need for the sql_* positional and --host arguments.')
    p.add_argument('sites_json', help='Flat JSON of TCCON site locations')
    p.add_argument('sqlite_db', help='Path to the sqlite3 file')
    p.add_argument('sql_db', nargs='?', help='Name of the MySQL database')
    p.add_argument('sql_user', nargs='?', help='MySQL username')
    p.add_argument('sql_pw', nargs='?', help='MySQL password')

    clargs = vars(p.parse_args())
    migrate(**clargs)


if __name__ == '__main__':
    main()
