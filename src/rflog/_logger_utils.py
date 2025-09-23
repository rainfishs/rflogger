import logging
import os
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import time

# from ..config import ERROR_FOLDER, LOG_ARCHIVE_DAY, LOG_FOLDER, LOG_LEVEL


class Formatter(logging.Formatter):

    def __init__(self, tz, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def ctr(timestamp: float | None):
            if timestamp is None:
                timestamp = time()
            # 轉換 UTC 時間為 UTC+8
            dt = datetime.fromtimestamp(timestamp, tz=tz)
            return dt.timetuple()  # 回傳 struct_time，符合 logging 格式需求

        self.converter = ctr

    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s.%03d"

    sys_path = sorted(sys.path, key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        module = record.pathname

        s = " ".join([
            f"{self.formatTime(record)}", f"{record.levelname:>8}",
            record.message,
            f"\t\t| {record.funcName}() at '{module}:{record.lineno}'"
        ])

        # Traceback
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s += "\n"
            s += record.exc_text

        # Stack
        if record.stack_info:
            if s[-1:] != "\n":
                s += "\n"
            s += self.formatStack(record.stack_info)

        return s


class Logger:

    def __init__(self,
                 tz: timezone = timezone(timedelta(hours=8)),
                 name: str = "main",
                 log_folder: str = "logs",
                 error_folder: str | None = None,
                 log_level: int = logging.INFO,
                 archive_days: int = 30):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(log_level)
        self._logger.propagate = False  # 避免重複輸出
        self._tz = tz
        self._archive_days = archive_days
        self._logger_handler = self._error_handler = None
        self.LOG_FOLDER = log_folder
        self.ERROR_FOLDER = error_folder if error_folder else os.path.join(
            log_folder, "errors")
        self.warm_up()

    def archive_old_logs(self):
        """
        壓縮超過 ARCHIVE_DAYS 天的 log 檔案。
        將一般 log 和 error log 分別壓縮到不同的 zip 檔案中。
        使用 'w' 模式重新建立 zip 檔案，並保留原先壓縮檔內的 log。
        """
        now = datetime.now(self._tz)
        cutoff_date = now.date() - timedelta(days=self._archive_days)
        LOG_FOLDER = self.LOG_FOLDER
        ERROR_FOLDER = self.ERROR_FOLDER
        log_archive_path = Path(LOG_FOLDER) / "archived_logs.zip"
        error_archive_path = Path(ERROR_FOLDER) / "archived_errors.zip"

        for log_folder, archive_path in [(LOG_FOLDER, log_archive_path),
                                         (ERROR_FOLDER, error_archive_path)]:
            log_path = Path(log_folder)
            if not log_path.exists():
                continue

            files_to_archive = []
            for log_file in log_path.glob("*.log"):
                if log_file.name.startswith(".latest"):  # 排除 .latest.log
                    continue

                try:
                    date_str = log_file.stem.split('_error')[
                        0]  # 處理 error log 名稱
                    file_date = datetime.strptime(date_str, "%Y%m%d").date()
                    if file_date <= cutoff_date:
                        files_to_archive.append(log_file)
                except ValueError:
                    logging.warning(f"無法解析檔案日期: {log_file.name}")
                    continue

            if files_to_archive:
                temp_archive_path = archive_path.with_suffix(
                    ".zip.temp")  # 建立暫時壓縮檔路徑
                existing_files_in_archive = []

                # 讀取現有壓縮檔內容 (如果存在)
                if archive_path.exists():
                    try:
                        with zipfile.ZipFile(archive_path, 'r') as zf_read:
                            existing_files_in_archive = zf_read.namelist()
                    except Exception as e:
                        logging.error(f"讀取現有壓縮檔 {archive_path} 時發生錯誤: {e}")
                        existing_files_in_archive = []  # 讀取失敗則清空，避免後續加入重複檔案

                try:
                    with zipfile.ZipFile(temp_archive_path, 'w',
                                         zipfile.ZIP_DEFLATED
                                         ) as zf_write:  # 使用 'w' 模式寫入暫時壓縮檔
                        # 先加入現有壓縮檔內的檔案
                        if existing_files_in_archive:
                            try:
                                with zipfile.ZipFile(
                                        archive_path,
                                        'r') as zf_read_old:  # 再次讀取舊的壓縮檔
                                    for filename in existing_files_in_archive:
                                        try:
                                            info = zf_read_old.getinfo(
                                                filename)  # 取得檔案資訊，保留 metadata
                                            data = zf_read_old.read(
                                                filename)  # 讀取檔案內容
                                            zf_write.writestr(info,
                                                              data)  # 寫入到新的壓縮檔
                                        except KeyError:
                                            logging.warning(
                                                f"無法在舊壓縮檔 {archive_path} 中找到檔案: {filename}"
                                            )
                            except Exception as e_old_zip:
                                logging.error(
                                    f"從舊壓縮檔 {archive_path} 複製檔案時發生錯誤: {e_old_zip}"
                                )

                        # 加入新的 log 檔案
                        for file_path in files_to_archive:
                            arcname = os.path.basename(
                                file_path)  # 保持壓縮檔內的路徑乾淨
                            if arcname not in existing_files_in_archive:  # 避免重複加入
                                zf_write.write(file_path, arcname=arcname)

                    # 替換舊的壓縮檔
                    if archive_path.exists():
                        os.remove(archive_path)  # 移除舊的壓縮檔
                    os.rename(temp_archive_path,
                              archive_path)  # 將暫時壓縮檔重新命名為正式壓縮檔名

                    logging.info(
                        f"已將 {len(files_to_archive)} 個 log 檔案壓縮至 {archive_path} (使用 'w' 模式重新建立)"
                    )

                    # 移除原始 log 檔案 (在壓縮成功後才移除)
                    for file_path in files_to_archive:
                        os.remove(file_path)

                except Exception as e:
                    logging.error(f"壓縮 log 檔案時發生錯誤: {e}")
                    if temp_archive_path.exists():  # 發生錯誤時移除暫時壓縮檔
                        os.remove(temp_archive_path)

    # 處理 .latest.log
    def warm_up(self):
        """
            啟動時檢查 .latest.log。可以知道該 latest 的創建時間，並決定要不要進行輪替。
            初次啟動時，若 .latest.log 不存在，則建立一個空的 .latest.log，並在第一行寫入創建時間。
        """
        LOG_FOLDER = self.LOG_FOLDER
        latest_log_path = Path(LOG_FOLDER) / ".latest.log"
        if not latest_log_path.exists():
            # 創建資料夾
            if not latest_log_path.parent.exists():
                latest_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(latest_log_path, "w", encoding="utf-8") as f:
                f.write(datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S\n"))
            self.create_handler()
        else:
            self.create_handler()
            self.rotate_log_files()

    def create_handler(self):
        # custom log handler
        LOG_FOLDER = self.LOG_FOLDER
        ERROR_FOLDER = self.ERROR_FOLDER
        LOG_LEVEL = self._logger.level
        logger_handler = logging.FileHandler(filename=Path(LOG_FOLDER) /
                                             ".latest.log",
                                             encoding="utf-8",
                                             mode="a")
        logger_handler.setFormatter(Formatter(self._tz))
        logger_handler.setLevel(LOG_LEVEL)
        # 確保 logs/error 資料夾存在
        if not Path(ERROR_FOLDER).exists():
            Path(ERROR_FOLDER).mkdir(parents=True, exist_ok=True)
        # custom error handler
        error_handler = logging.FileHandler(filename=Path(ERROR_FOLDER) /
                                            ".latest_error.log",
                                            encoding="utf-8",
                                            mode="a")
        error_handler.setFormatter(Formatter(self._tz))
        error_handler.setLevel(logging.ERROR)

        self._logger.addHandler(logger_handler)  # bind to logger: main
        self._logger.addHandler(error_handler)  # bind to logger: main

        logging.getLogger().addHandler(error_handler)  # bind to root logger

        self._logger_handler = logger_handler
        self._error_handler = error_handler

    def close_handler(self):
        if self._logger_handler:
            self._logger.removeHandler(self._logger_handler)
            self._logger_handler.close()
        if self._error_handler:
            logging.getLogger().removeHandler(self._error_handler)
            self._logger.removeHandler(self._error_handler)
            self._error_handler.close()
        self._logger_handler = self._error_handler = None

    def rotate_log_files(self):
        """
        輪替 log 檔案。
        將 .latest.log 和 .latest_error.log 重新命名為 YYYYMMDD.log 和 YYYYMMDD_error.log。
        """
        LOG_FOLDER = self.LOG_FOLDER
        ERROR_FOLDER = self.ERROR_FOLDER
        latest_log_path = Path(LOG_FOLDER) / ".latest.log"
        latest_error_log_path = Path(ERROR_FOLDER) / ".latest_error.log"

        if not latest_log_path.exists() or not latest_error_log_path.exists():
            logging.error(".latest.log 或 .latest_error.log 不存在")
            return

        # 讀取第一行的創建時間
        with open(latest_log_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            try:
                latest_log_ctime = datetime.strptime(first_line,
                                                     "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logging.error(f"無法解析 .latest.log 的創建時間: {first_line}")
                return

        # 如果是今天的 log，就 return 不用輪替
        latest_log_date = latest_log_ctime.date()
        today = datetime.now(self._tz).date()

        if latest_log_date == today:
            return

        # 關閉目前的 handler
        self.close_handler()

        previous_date = latest_log_date.strftime("%Y%m%d")
        # 重新命名 .latest.log -> YYYYMMDD.log 和 .latest_error.log -> YYYYMMDD_error.log
        latest_log_path = Path(LOG_FOLDER) / ".latest.log"
        latest_error_log_path = Path(ERROR_FOLDER) / ".latest_error.log"

        rotated_log_path = Path(LOG_FOLDER) / f"{previous_date}.log"
        rotated_error_log_path = Path(
            ERROR_FOLDER) / f"{previous_date}_error.log"

        latest_log_path.rename(rotated_log_path)
        latest_error_log_path.rename(rotated_error_log_path)

        # 在 .latest.log 寫入新的創建時間
        with open(latest_log_path, "w", encoding="utf-8") as f:
            f.write(datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S\n"))

        # 壓縮舊的 log 檔案
        self.archive_old_logs()

        # 重新建立 handler
        self.create_handler()

    @property
    def main_logger(self):
        return self._logger
