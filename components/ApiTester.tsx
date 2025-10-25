import React, { useState } from 'react';

interface VideoFormat {
  format_id: string;
  resolution: string;
  height: number;
  fps: number;
  filesize: number;
  ext: string;
  vcodec: string;
  acodec: string;
}

interface VideoInfo {
  title: string;
  duration: number;
  thumbnail: string;
  formats: VideoFormat[];
}

const ApiTester: React.FC = () => {
  const [url, setUrl] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isFetchingFormats, setIsFetchingFormats] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<string | null>(null);
  const [videoInfo, setVideoInfo] = useState<VideoInfo | null>(null);
  const [selectedFormat, setSelectedFormat] = useState<string | null>(null);
  const [downloadProgress, setDownloadProgress] = useState<string>('');

  const isValidUrl = (urlString: string) => {
    try {
      new URL(urlString);
      return true;
    } catch (_) {
      return false;
    }
  };

  const formatFileSize = (bytes: number): string => {
    if (!bytes || bytes === 0) return 'Unknown';
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  };

  const handleFetchFormats = async () => {
    if (!apiKey.trim()) {
      setError('Please provide an API Key first.');
      return;
    }

    if (!url || !isValidUrl(url)) {
      setError('Please enter a valid video URL.');
      return;
    }

    setIsFetchingFormats(true);
    setError(null);
    setVideoInfo(null);
    setSelectedFormat(null);

    try {
      const response = await fetch('/v1/formats', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ url }),
      });

      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.error || 'Failed to fetch formats');
      }

      const data = await response.json();
      setVideoInfo(data);
      setDownloadProgress('Formats loaded. Select a quality to download.');
      
    } catch (err: any) {
      setError(err.message || 'Failed to fetch video formats');
    } finally {
      setIsFetchingFormats(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setResult(null);

    if (!apiKey.trim()) {
      setError('Please provide an API Key. You can generate one from the "API Key Management" tab.');
      return;
    }

    if (!videoInfo || !selectedFormat) {
      setError('Please fetch formats and select a quality first.');
      return;
    }

    setIsLoading(true);
    setDownloadProgress('Starting download...');

    try {
      setDownloadProgress('Downloading video...');
      
      const response = await fetch('/v1/download', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ 
          url: url,
          format_id: selectedFormat
        }),
      });

      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.error || `Request failed with status ${response.status}`);
      }
      
      setDownloadProgress('Processing and merging video...');
      
      const blob = await response.blob();
      const contentDisposition = response.headers.get('content-disposition');
      let fileName = 'download.mp4';
      if (contentDisposition) {
        const fileNameMatch = contentDisposition.match(/filename="(.+)"/);
        if (fileNameMatch && fileNameMatch.length === 2) {
          fileName = fileNameMatch[1];
        }
      }

      setDownloadProgress('Download complete! Saving file...');
      
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = downloadUrl;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(downloadUrl);

      setResult(fileName);
      setDownloadProgress('');
      setVideoInfo(null);
      setSelectedFormat(null);
      
    } catch (err: any) {
       setError(err.message || 'An unexpected error occurred. Is the backend server running?');
       setDownloadProgress('');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="p-6 bg-surface/50 rounded-xl border border-border">
      <h2 className="text-2xl font-bold text-text-primary mb-1">API Tester</h2>
      <p className="text-text-secondary mb-6">Enter your API key and video URL, then select quality.</p>

      <div className="space-y-4">
        <div>
            <label htmlFor="apiKey" className="block text-sm font-medium text-text-secondary mb-1">API Key</label>
            <input
                id="apiKey"
                type="text"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="Paste a generated API key here..."
                className="w-full bg-surface border border-border rounded-md px-4 py-2 text-text-primary focus:ring-2 focus:ring-primary focus:outline-none transition-shadow"
                disabled={isLoading || isFetchingFormats}
            />
        </div>

        <div>
            <label htmlFor="videoUrl" className="block text-sm font-medium text-text-secondary mb-1">Video URL</label>
            <div className="flex flex-col sm:flex-row gap-2">
              <input
                id="videoUrl"
                type="text"
                value={url}
                onChange={(e) => {
                  setUrl(e.target.value);
                  setVideoInfo(null);
                  setSelectedFormat(null);
                }}
                placeholder="https://www.youtube.com/watch?v=..."
                className="flex-grow bg-surface border border-border rounded-md px-4 py-2 text-text-primary focus:ring-2 focus:ring-primary focus:outline-none transition-shadow"
                disabled={isLoading || isFetchingFormats}
              />
              <button
                type="button"
                onClick={handleFetchFormats}
                disabled={isFetchingFormats || isLoading || !url || !apiKey}
                className="bg-primary hover:bg-indigo-500 text-white font-bold py-2 px-6 rounded-md transition-all duration-200 ease-in-out disabled:bg-gray-500 disabled:cursor-not-allowed flex items-center justify-center whitespace-nowrap"
              >
                {isFetchingFormats ? (
                  <>
                    <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    Fetching...
                  </>
                ) : (
                  'Get Quality Options'
                )}
              </button>
            </div>
        </div>
        
        {error && <p className="text-red-400 mt-2 text-sm">{error}</p>}
        {downloadProgress && <p className="text-blue-400 mt-2 text-sm animate-pulse">{downloadProgress}</p>}
      </div>

      {videoInfo && (
        <div className="mt-6 p-4 bg-surface rounded-lg border border-border">
          <div className="flex items-start gap-4 mb-4">
            {videoInfo.thumbnail && (
              <img src={videoInfo.thumbnail} alt="Video thumbnail" className="w-32 h-20 object-cover rounded" />
            )}
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-text-primary mb-1">{videoInfo.title}</h3>
              <p className="text-sm text-text-secondary">
                Duration: {Math.floor(videoInfo.duration / 60)}:{(videoInfo.duration % 60).toString().padStart(2, '0')}
              </p>
            </div>
          </div>

          <div className="mb-4">
            <label className="block text-sm font-medium text-text-secondary mb-2">Select Quality:</label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-h-64 overflow-y-auto">
              {videoInfo.formats.map((format) => (
                <label
                  key={format.format_id}
                  className={`flex items-center justify-between p-3 rounded-md border cursor-pointer transition-colors ${
                    selectedFormat === format.format_id
                      ? 'border-primary bg-primary/20'
                      : 'border-border hover:border-primary/50 hover:bg-surface'
                  }`}
                >
                  <div className="flex items-center gap-3">
                    <input
                      type="radio"
                      name="quality"
                      value={format.format_id}
                      checked={selectedFormat === format.format_id}
                      onChange={(e) => setSelectedFormat(e.target.value)}
                      className="text-primary focus:ring-primary"
                    />
                    <div>
                      <div className="text-text-primary font-medium">{format.resolution}</div>
                      <div className="text-xs text-text-secondary">{format.fps} FPS • {format.ext}</div>
                    </div>
                  </div>
                  <div className="text-sm text-text-secondary">
                    {formatFileSize(format.filesize)}
                  </div>
                </label>
              ))}
            </div>
          </div>

          <button
            onClick={handleSubmit}
            disabled={isLoading || !selectedFormat}
            className="w-full bg-secondary hover:bg-emerald-500 text-white font-bold py-3 px-6 rounded-md transition-all duration-200 ease-in-out disabled:bg-gray-500 disabled:cursor-not-allowed flex items-center justify-center"
          >
            {isLoading ? (
              <>
                <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Processing...
              </>
            ) : (
              'Download & Merge Selected Quality'
            )}
          </button>
        </div>
      )}

      {result && (
        <div className="mt-6 p-4 bg-surface rounded-lg border border-border animate-fade-in">
          <h3 className="text-lg font-semibold text-green-400 mb-2">✓ Download Complete!</h3>
           <div className="space-y-2 text-sm">
            <p><strong className="text-text-secondary">Downloaded File:</strong> <span className="font-mono">{result}</span></p>
          </div>
        </div>
      )}
    </div>
  );
};

export default ApiTester;