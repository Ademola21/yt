
export interface ApiKey {
  id: string;
  key: string;
  createdAt: string;
}

export interface DownloadResult {
  downloadUrl: string;
  fileName: string;
  size: string;
  duration: string;
}
