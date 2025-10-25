
import React from 'react';
import { useCopyToClipboard } from '../hooks/useCopyToClipboard';
import ClipboardIcon from './icons/ClipboardIcon';
import CheckIcon from './icons/CheckIcon';

const CodeBlock: React.FC<{ code: string; language: string }> = ({ code, language }) => {
    const [isCopied, copy] = useCopyToClipboard();
    return (
        <div className="bg-gray-900 rounded-lg my-4 relative group">
            <button
                onClick={() => copy(code)}
                className="absolute top-2 right-2 p-2 bg-gray-700 rounded-md text-text-secondary opacity-0 group-hover:opacity-100 transition-opacity"
                aria-label="Copy code snippet"
            >
                {isCopied ? <CheckIcon className="w-5 h-5 text-green-400"/> : <ClipboardIcon className="w-5 h-5"/>}
            </button>
            <pre className="p-4 overflow-x-auto">
                <code className={`language-${language} text-sm`}>{code}</code>
            </pre>
        </div>
    );
};

const ApiDocumentation: React.FC = () => {
    const curlExample = `curl -X POST 'https://your-api-domain.com/v1/download' \\
-H 'Authorization: Bearer YOUR_API_KEY' \\
-H 'Content-Type: application/json' \\
-d '{
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  "audio_format": "libfdk_aac",
  "audio_bitrate": "30k"
}'`;

    const fetchExample = `const videoUrl = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ';
const apiKey = 'YOUR_API_KEY';

fetch('https://your-api-domain.com/v1/download', {
  method: 'POST',
  headers: {
    'Authorization': \`Bearer \${apiKey}\`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    url: videoUrl,
    audio_format: 'libfdk_aac',
    audio_bitrate: '30k' // For HE-AAC quality
  })
})
.then(response => {
  if (!response.ok) {
    throw new Error('Network response was not ok');
  }
  return response.json();
})
.then(data => {
  console.log('Download job created:', data);
  // data will contain info like { jobId, status, downloadUrl }
  window.location.href = data.downloadUrl;
})
.catch(error => {
  console.error('There was a problem with the fetch operation:', error);
});`;

  return (
    <div className="p-6 bg-surface/50 rounded-xl border border-border">
      <h2 className="text-2xl font-bold text-text-primary">API Documentation</h2>
      <p className="text-text-secondary mt-1 mb-6">
        Integrate our video processing API into your application.
      </p>

      <section className="mb-8">
        <h3 className="text-xl font-semibold text-text-primary mb-2">Authentication</h3>
        <p className="text-text-secondary">
          All API requests must be authenticated using a Bearer token in the
          <code className="bg-gray-900 text-sm font-mono p-1 rounded-md mx-1">Authorization</code> header. 
          Generate your keys in the "API Key Management" tab.
        </p>
      </section>

      <section className="mb-8">
        <h3 className="text-xl font-semibold text-text-primary mb-2">Endpoint: <code className="bg-primary/20 text-secondary p-1 rounded-md text-base">POST /v1/download</code></h3>
        <p className="text-text-secondary mb-4">
          This endpoint initiates a download and merge job. The process is asynchronous. You will receive a job ID in response, which can be used to check the status, but for simplicity, this example returns a direct download URL upon completion.
        </p>
        
        <h4 className="font-semibold text-text-primary mb-2">Request Body (JSON)</h4>
        <ul className="list-disc list-inside text-text-secondary space-y-1 pl-2">
            <li><code className="font-mono">url</code> (string, required): The URL of the video to process.</li>
            <li><code className="font-mono">audio_format</code> (string, optional): The desired audio codec. We highly recommend <code className="bg-gray-900 text-sm font-mono p-1 rounded-md">'libfdk_aac'</code> for high-quality HE-AAC audio.</li>
            <li><code className="font-mono">audio_bitrate</code> (string, optional): The target audio bitrate, e.g., <code className="bg-gray-900 text-sm font-mono p-1 rounded-md">'30k'</code>.</li>
        </ul>
      </section>

      <section className="mb-8">
        <h3 className="text-xl font-semibold text-text-primary mb-2">Example: cURL</h3>
        <p className="text-text-secondary mb-2">
            A quick way to test the API from your terminal.
        </p>
        <CodeBlock code={curlExample} language="bash" />
      </section>

      <section>
        <h3 className="text-xl font-semibold text-text-primary mb-2">Example: JavaScript (Fetch API)</h3>
        <p className="text-text-secondary mb-2">
            Use this in your web application to call the API.
        </p>
        <CodeBlock code={fetchExample} language="javascript" />
      </section>
    </div>
  );
};

export default ApiDocumentation;
