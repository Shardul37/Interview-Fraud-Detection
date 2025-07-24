# WavLM Audio Analysis - Test Setup

To run the code:
Open 4 terminals.

Run the main file in one
Simulate_video_ready_producer in another- A dummy script to pass messages to queue
video_cnverter_consumer in another - converts video to audio
queue_monitor keeps track of converted audio files for inferencing.
On the vPN and open the mongoDB