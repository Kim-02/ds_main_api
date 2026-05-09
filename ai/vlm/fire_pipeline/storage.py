import json
import os

import cv2


class RollingStorage:
    def __init__(self, frame_folder, summary_folder, frame_keep_count, summary_keep_count):
        self.frame_folder = frame_folder
        self.summary_folder = summary_folder
        self.frame_keep_count = frame_keep_count
        self.summary_keep_count = summary_keep_count
        self.saved_frame_files = []
        self.saved_summary_files = []

        os.makedirs(self.frame_folder, exist_ok=True)
        os.makedirs(self.summary_folder, exist_ok=True)

    def clean_start_files(self):
        for file_name in os.listdir(self.frame_folder):
            if file_name.startswith("main_frame_"):
                os.remove(f"{self.frame_folder}/{file_name}")

        for file_name in os.listdir(self.summary_folder):
            if file_name.startswith("vlm_summary_"):
                os.remove(f"{self.summary_folder}/{file_name}")

    def save_frame(self, frame_number, analyzed_frame):
        save_path = f"{self.frame_folder}/main_frame_{frame_number:06d}.jpg"
        cv2.imwrite(save_path, analyzed_frame)
        self.saved_frame_files.append(save_path)

        if len(self.saved_frame_files) > self.frame_keep_count:
            old_file = self.saved_frame_files.pop(0)

            if os.path.exists(old_file):
                os.remove(old_file)

        return save_path

    def save_summary(self, summary_number, summary, text):
        json_path = f"{self.summary_folder}/vlm_summary_{summary_number:03d}.json"
        txt_path = f"{self.summary_folder}/vlm_summary_{summary_number:03d}.txt"

        with open(json_path, "w", encoding="utf-8") as file:
            json.dump(summary, file, ensure_ascii=False, indent=2)

        with open(txt_path, "w", encoding="utf-8") as file:
            file.write(text)

        self.saved_summary_files.append((json_path, txt_path))

        if len(self.saved_summary_files) > self.summary_keep_count:
            old_json_path, old_txt_path = self.saved_summary_files.pop(0)

            if os.path.exists(old_json_path):
                os.remove(old_json_path)

            if os.path.exists(old_txt_path):
                os.remove(old_txt_path)

        return json_path, txt_path
