#!/usr/bin/env bash

export PATH=$PATH:/root/.dotnet

cd /root
/usr/bin/bash ixiAutoAccept.sh &
/usr/bin/bash ixiMessageHandler.sh &

cd /root/QuIXI/QuIXI/bin/Release/net10.0/
dotnet QuIXI.dll --walletPassword YOUR_WALLET_PASSWORD
