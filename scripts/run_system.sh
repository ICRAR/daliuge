echo "Starting DALiuGE system:"
echo ""
./start_translator.sh
./start_daemon.sh
./start_dim.sh
./start_nm.sh
./stop_engine.sh
./stop_translator.sh
echo "DALiuGE system terminated"
