DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

kitchen.sh -norep=Y -file="$DIR/nightly_processing.kjb" 
