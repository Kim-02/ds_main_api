import cv2
import os
import time


class VideoSource:
    def __init__(self, source):
        self.source = source
        self.cap = None
        self.start_time = 0

    def is_live_source(self):
        return str(self.source).startswith("rtsp://")

    def open(self):
        if self.is_live_source():
            os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|max_delay;0"

        if self.is_live_source():
            self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)
        else:
            self.cap = cv2.VideoCapture(self.source)
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self.start_time = time.monotonic()
        return self.cap.isOpened()

    def read(self):
        return self.cap.read()

    def get_fps(self):
        fps = self.cap.get(cv2.CAP_PROP_FPS)

        if fps == 0:
            return 30

        return fps

    def get_time(self):
        if self.is_live_source():
            return time.monotonic() - self.start_time

        current_time = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000

        if current_time == 0:
            return time.monotonic() - self.start_time

        return current_time

    def release(self):
        if self.cap is not None:
            self.cap.release()
