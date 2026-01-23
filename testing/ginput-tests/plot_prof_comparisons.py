from argparse import ArgumentParser
from datetime import datetime
from ginput.common_utils import readers
import matplotlib.pyplot as plt
from pathlib import Path
import re
import tarfile
import tempfile


GAS_SCALES = {
    'co2': (1e6, 'ppm'),
    'ch4': (1e9, 'ppb'),
    'co': (1e9, 'ppb')
}


def main():
    clargs = parse_args()
    tar_path = clargs['tar_file']
    comp_dir = clargs['comparison_dir']
    gases = clargs['gases'].split(',')
    top_alt = clargs['top_alt']
    nx = len(gases)
    with tempfile.TemporaryDirectory(dir='.') as tdir:
        vmr_files = extract_vmr_files(tar_path, Path(tdir), date_range=clargs['date_range'])
        comp_vmr_files = [comp_dir / f.name for f in vmr_files]

        _, axs = plt.subplots(3, nx, figsize=(6*nx, 12), gridspec_kw={'hspace': 0.4})
        plot_gases_for_files(vmr_files, gases, top_alt, axs[0])
        for ax in axs[0]:
            ax.set_title('Automation')
        plot_gases_for_files(comp_vmr_files, gases, top_alt, axs[1])
        for ax in axs[1]:
            ax.set_title('Comparison')
        plot_gas_differences_for_files(vmr_files, comp_vmr_files, gases, top_alt, axs[2])


    plt.savefig(f'{tar_path.stem}-vs-{comp_dir.name}-vmr-comparison.pdf', bbox_inches='tight')


def parse_args():
    def date_range(s):
        s1, s2 = s.split('-')
        d1 = datetime.strptime(s1, '%Y%m%d')
        d2 = datetime.strptime(s2, '%Y%m%d')
        return (d1, d2)

    p = ArgumentParser(description='Plot comparisons of .vmr files')
    p.add_argument('tar_file', type=Path, help='Path to the .tar file with the .vmr files')
    p.add_argument('comparison_dir', type=Path, help='Path to the directory with .vmr files to compare against')
    p.add_argument('--gases', default='co2,ch4,co', help='Comma-separated list of gases to plot')
    p.add_argument('--top-alt', default=20.0, type=float, help='Top altitude in kilometers to plot')
    p.add_argument('--date-range', type=date_range, help='The range of dates in the tar file to compare, as YYYYMMDD-YYYYMMDD (end is exclusive)') 

    return vars(p.parse_args())


def extract_vmr_files(tar_path: Path, dest_dir: Path, date_range=None) -> list[Path]:
    """
    Extracts all .vmr files from a tar archive into a single destination 
    directory, removing any internal folder structure.
    """
    vmr_files = []
    with tarfile.open(tar_path, 'r:*') as tar:
        for member in tar.getmembers():
            # Check if it's a file and ends with .vmr
            if member.isfile() and member.name.lower().endswith('.vmr'):
                # Get just the filename (e.g., 'path/to/data.vmr' -> 'data.vmr')
                filename = Path(member.name).name
                if date_range is not None:
                    d1, d2 = date_range
                    filedate = re.search(r'(\d{10})Z', filename).group(1)
                    filedate = datetime.strptime(filedate, '%Y%m%d%H')
                    if filedate < d1 or filedate >= d2:
                        # Don't extract a file outside the specified date range
                        continue
                dest_path = dest_dir / filename

                # Extract the file object and write it to the new destination
                with tar.extractfile(member) as source_file:
                    with open(dest_path, 'wb') as target_file:
                        target_file.write(source_file.read())

                print(f"Extracted: {filename}")
                vmr_files.append(dest_dir / filename)
    return vmr_files


def plot_gases_for_files(vmr_files, gases, top_alt, axs):
    for file in vmr_files:
        hr = re.search(r'\d\dZ', file.name).group()
        data = readers.read_vmr_file(file)
        zz = data['profile']['altitude'] <= top_alt
        for igas, gas in enumerate(gases):
            scale, unit = GAS_SCALES.get(gas, (1.0, 'mol/mol'))
            ax = axs[igas]
            ax.plot(data['profile'][gas][zz] * scale, data['profile']['altitude'][zz], label=hr)
            ax.set_xlabel(f'{gas.upper()} ({unit})')
            ax.set_ylabel('Altitude (km)')

    for ax in axs:
        ax.legend(fontsize=6, ncol=2)


def plot_gas_differences_for_files(vmr_files, comp_vmr_files, gases, top_alt, axs):
    for auto_file, comp_file in zip(vmr_files, comp_vmr_files):
        hr = re.search(r'\d\dZ', auto_file.name).group()
        chk_hr = re.search(r'\d\dZ', comp_file.name).group()
        if hr != chk_hr:
            raise ValueError('Automation and comparison VMR files not aligned')

        auto_data = readers.read_vmr_file(auto_file)
        comp_data = readers.read_vmr_file(comp_file)
        zz = auto_data['profile']['altitude'] <= top_alt
        for igas, gas in enumerate(gases):
            scale, unit = GAS_SCALES.get(gas, (1.0, 'mol/mol'))
            ax = axs[igas]
            delta = auto_data['profile'][gas] - comp_data['profile'][gas]
            ax.plot(delta[zz] * scale, auto_data['profile']['altitude'][zz], label=hr)
            ax.set_xlabel(f'{gas.upper()} ({unit})')
            ax.set_ylabel('Altitude (km)')

    for ax in axs:
        ax.legend(fontsize=6, ncol=2)


if __name__ == '__main__':
    main()
