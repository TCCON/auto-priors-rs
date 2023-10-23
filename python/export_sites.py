from datetime import datetime, timedelta
import json


_now_cutoff = datetime.now() - timedelta(hours=1)
_date_fmt = '%Y-%m-%d'

def save_sites_as_flat_json(sites_in: dict, json_file: str):
    sites_out = []
    for sid, info in sites_in.items():
        for (start, end), loc in info['time_spans'].items():
            sites_out.append({
                'site_id': sid,
                'name': info['name'],
                'location': info['loc'],
                'start_date': start.strftime(_date_fmt),
                'end_date': None if end > _now_cutoff else end.strftime(_date_fmt),
                'latitude': loc['lat'],
                'longitude': loc['lon'] if loc['lon'] <= 180.0 else loc['lon'] - 360.0,
                'altitude': loc['alt'],
                'comment': ''
            })
    with open(json_file, 'w') as f:
        json.dump(sites_out, f, indent=2)


if __name__ == '__main__':
    from ginput.mod_maker import tccon_sites
    save_sites_as_flat_json(tccon_sites.site_dict, 'ginput_sites.json')
