from argparse import ArgumentParser
import enum
import sqlalchemy as db
from sqlalchemy import orm, sql

BaseSqlite = orm.declarative_base()
BaseSql = orm.declarative_base()

# TODO: Jobs table (simple mapping between sqlite3 and sql)
# TODO: StdSites table (more complex, needs to be split and reorganized)

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


class GeosFilesSql(BaseSql):
    __tablename__ = 'GeosFiles'

    file_id = db.Column(db.Integer, primary_key=True)
    file_path = db.Column(db.Text)
    product = db.Column(db.String(8))
    filedate = db.Column(db.DATETIME)
    levels = db.Column(db.String(8))
    data_type = db.Column(db.String(8))

    def __repr__(self) -> str:
        return f'GeosFilesSql(file_id = {self.file_id}, file_path = {self.file_path}, product = {self.product}, filedate = {self.filedate}, levels = {self.levels}, type = {self.data_type})'

    @classmethod
    def from_sqlite(cls, obj):
        return cls(file_id=obj.file_id, file_path=obj.path, product=obj.product, filedate=obj.filedate, levels=obj.levels, data_type=obj.type)


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
    site_id = db.Column(db.Text)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    lat = db.Column(db.Text)
    lon = db.Column(db.Text)
    email = db.Column(db.String(64))
    delete_time = db.Column(db.DateTime)
    priority = db.Column(db.Integer)
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
        return cls(job_id=obj.job_id, state=obj.state, site_id=obj.site_id, start_date=obj.start_date, end_date=obj.end_date,
                   lat=obj.lat, lon=obj.lon, email=obj.email, delete_time=obj.delete_time, priority=obj.priority, save_dir=obj.save_dir,
                   save_tarball=obj.save_tarball, mod_fmt=obj.mod_fmt, map_fmt=obj.map_fmt, vmr_fmt=obj.vmr_fmt, submit_time=obj.submit_time,
                   complete_time=obj.complete_time, output_file=obj.output_file)


class StdSiteSqlite(BaseSqlite):
    __tablename__ = 'StdSites'

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    job_id = db.Column(db.Integer)
    day_state = db.Column(db.Integer)


class StdSiteSql(BaseSql):
    __tablename__ = 'StdSiteList'

    id = db.Column(db.Integer, primary_key=True)
    site_id = db.Column(db.String(2))


class StdSiteJob(BaseSql):
    __tablename__ = 'StdSiteJobs'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    site = db.Column(db.Integer)
    date = db.Column(db.Date)
    state = db.Column(db.Integer)
    job = db.Column(db.Integer)

    def __repr__(self) -> str:
        return f'StdSiteJob({self.site} {self.date}: job {self.job}, state {self.state}'



def migrate(sqlite_db, sql_db, sql_user, sql_pw):
    sqlite_engine = db.create_engine(f'sqlite:///{sqlite_db}', future=True)
    mysql_engine = db.create_engine(f'mysql://{sql_user}:{sql_pw}@localhost/{sql_db}', future=True)
    # migrate_table(sqlite_engine, mysql_engine, GeosPathsSqlite, GeosPathsSql)
    # migrate_table(sqlite_engine, mysql_engine, GeosFilesSqlite, GeosFilesSql)
    # migrate_table(sqlite_engine, mysql_engine, JobsSqlite, JobsSql)
    migrate_std_sites(sqlite_engine, mysql_engine)


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
    with orm.Session(mysql_engine) as s_mysql:
        s_mysql.execute(db.delete(StdSiteJob))
        s_mysql.execute(db.delete(StdSiteSql))
        for i, site in enumerate(sites, start=1):
            o = StdSiteSql(id=i, site_id=site)
            s_mysql.add(o)
            site_mapping[site] = i

        s_mysql.commit()

    with orm.Session(sqlite_engine) as s_lite, orm.Session(mysql_engine) as s_mysql:
        

        result = s_lite.execute(sql.text('SELECT * FROM StdSites'))
        
        for i, row in enumerate(result, start=1):
            print(f'\rCopying row {i} from StdSites', end='')
            site_states = []
            for site in sites:
                site_fk = site_mapping[site]
                state = row[site]
                job_id = row['job_id'] if state >= 0 else None
                site_states.append(StdSiteJob(site=site_fk, date=row['date'], job=job_id, state=state))
            s_mysql.add_all(site_states)
            s_mysql.commit()
        print('\nDone.')


def main():
    p = ArgumentParser('Migrate an AutoModPython sqlite3 database to MySQL')
    p.add_argument('sqlite_db', help='Path to the sqlite3 file')
    p.add_argument('sql_db', help='Name of the MySQL database')
    p.add_argument('sql_user', help='MySQL username')
    p.add_argument('sql_pw', help='MySQl password')

    clargs = vars(p.parse_args())
    migrate(**clargs)


if __name__ == '__main__':
    main()
