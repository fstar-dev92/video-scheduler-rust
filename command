gst-launch-1.0     udpsrc address=239.1.1.1 port=5002 caps="application/x-rtp" !     rtpjitterbuffer latency=200 !     rtpmp2tdepay !     tsdemux name=demux     demux. ! queue ! h264parse ! avdec_h264 ! videoconvert ! x264enc tune=zerolatency ! h264parse ! mpegtsmux name=mux !     rtpmp2tpay mtu=1400 pt=33 !     udpsink host=239.2.2.2 port=6000 auto-multicast=true     demux. ! queue ! aacparse ! avdec_aac ! audioconvert ! voaacenc ! aacparse ! mux.


//only without transcoding
gst-launch-1.0     udpsrc address=239.1.1.1 port=5002 !     udpsink host=239.2.2.2 port=6000 auto-multicast=true