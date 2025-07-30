#This codebase contains two separate services.
The monitoring.video_converter_consumer converts video into audio segments. 
The monitoring.ml_batch_processor.py to process these audio segments to give inference. 
simulate_video_ready_producer sends dummy messages to the rabbitmq saying follwing video is stored in gcs and interview_id is ready for audio extracion

History is tracked and updated in mongoDB. The extracted audiofiles are stored in GCS. I am not deleting them yet. FInal result from ml inferencing is stored in same collection. 

To create docker images run those two scripts. The ml-service container may have problem to run in a 8GB machine as the code itself demands 6-8GB of ram. 

You can edit parameters like how many segments you want to process in a single ml pass, url for rabbitmq or gcs/mongdb destinations
