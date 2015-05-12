rm hadoo-prelease
rm *.xml

pex . -o hazoo.pex -c hazoo

./hazoo.pex create_workernode ./hazoo.pex create_workernode --nm_web_port=1 --nm_main_port=2 --nm_loc_port=3 --nm_shuffle_port=4 --dn_web_port=5 --dn_ipc_port=6 -- dn_rpc_port=7 sdzookeeper.local.twitter.com sdzktest.smf1.twitter.com
