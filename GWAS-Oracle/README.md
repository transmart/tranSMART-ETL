* Use an up-to-date version of transmart-gwas-plugin (post Aug 12 2015).  Use
  the gwava branch of transmart-data.  You'll have to build the GWAVA war:
  https://github.com/transmart/GWAVA . When editing the build file, add
  http://localhost:9090/gwava to the webroots and deploy the WAR on that port.
* On transmart-data/config/Config-extra.php, refer to this location:
```
com { recomdata { rwg { webstart {
	    codebase = "http://localhost:9090/gwava"
} } } }
```
* Redeploy the config file: `make -C config install_Config.groovy`.

Roughly follow the instructions at [Rancho\_GWAS\_ETL.pdf](Rancho_GWAS_ETL.pdf).

The instructions won't exactly match because there've been some changes:

* Some settings were moved to a vars file that you can source
* `1.drop_n_dumps.sh` was renamed `1.load_dumps.sh` as it doesn't drop tables
  anymore.  The grants script was removed, so the 3. and 4. scripts were
  renamed 2. and 3.
* At the end, update the solr index manually (e.g. with  `make -C solr rwg_full_import`)

(For The Hyve people: see
https://gist.github.com/cataphract/140082cdb9e33d7cf222 for how to fetch an
Oracle image. Unless you want to wait 10 more times than needed, don't run
docker on a virtualized disk and use either the devicemapper backend against a
*real* block device (not the LVM-over-loop-device-over-sparse-file default) or
btrfs (which is simpler if you're already using btrfs as you don't have to
repartition)
