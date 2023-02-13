
import xarray as xr


def main():
    years = range(2000, 2022)

    subset = dict(
        longitude=slice(60, 70),
        latitude=slice(50, 40),
    )

    mask = xr.open_dataset(
        '/Net/Groups/data_BGC/era5/e1/0d25_daily/WAI1/WAI1.1979.nc').WAI1.isel(time=0).notnull().rename(lat='latitude', lon='longitude').sel(**subset).compute()

    def apply_mask(ds, mask, subset):
        ds_sub = ds.sel(**subset)
        mask_sub = mask.sel(**subset)
        return ds_sub.where(mask_sub)

    def preproc(x: xr.Dataset):

        x = apply_mask(x, mask, subset)

        for var in x.data_vars:
            if var == 't2m':
                x[var] = x[var] - 273.15
            elif var == 'sd':
                x[var] = x[var] * 1000

        return x

    tair = xr.open_mfdataset([f'/Net/Groups/data_BGC/era5/e1/0d25_daily/t2m/t2m.daily.an.era5.1440.720.{year:4d}.nc' for year in years], preprocess=preproc, parallel=True)
    tp = xr.open_mfdataset([f'/Net/Groups/data_BGC/era5/e1/0d25_daily/tp/tp.daily.fc.era5.1440.720.{year:4d}.nc' for year in years], preprocess=preproc, parallel=True)
    sd = xr.open_mfdataset([f'/Net/Groups/data_BGC/era5/e1/0d25_daily/sd/sd.daily.an.era5.1440.720.{year:4d}.nc' for year in years], preprocess=preproc, parallel=True)
    ssrd = xr.open_mfdataset([f'/Net/Groups/data_BGC/era5/e1/0d25_daily/ssrd/ssrd.daily.fc.era5.1440.720.{year:4d}.nc' for year in years], preprocess=preproc, parallel=True)

    ds = xr.Dataset()
    ds['tair'] = tair.t2m
    ds['tp'] = tp.tp
    ds['swe'] = sd.sd
    ds['ssrd'] = ssrd.ssrd
    ds['mask'] = mask

    ds.to_netcdf('/Net/Groups/BGI/scratch/bkraft/swe_hybrid/era_swe.nc')

if __name__ == '__main__':
    main()
