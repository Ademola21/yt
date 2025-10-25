import React, { useState, useEffect } from 'react';
import type { ApiKey } from '../types';
import { useCopyToClipboard } from '../hooks/useCopyToClipboard';
import ClipboardIcon from './icons/ClipboardIcon';
import CheckIcon from './icons/CheckIcon';

const ApiKeyRow: React.FC<{ apiKey: ApiKey }> = ({ apiKey }) => {
  const [isCopied, copy] = useCopyToClipboard();

  return (
    <div className="flex items-center justify-between bg-surface p-4 rounded-lg hover:bg-gray-700/50 transition-colors">
      <div className="flex-1 overflow-hidden">
        <p className="font-mono text-sm text-text-primary truncate">{apiKey.key}</p>
        <p className="text-xs text-text-secondary mt-1">Created on {apiKey.createdAt}</p>
      </div>
      <button
        onClick={() => copy(apiKey.key)}
        className="ml-4 p-2 rounded-md bg-gray-600 hover:bg-primary transition-colors text-text-primary disabled:opacity-50"
        aria-label="Copy API Key"
      >
        {isCopied ? <CheckIcon className="w-5 h-5 text-green-400" /> : <ClipboardIcon className="w-5 h-5" />}
      </button>
    </div>
  );
};

const ApiKeyManager: React.FC = () => {
  const [apiKeys, setApiKeys] = useState<ApiKey[]>([]);
  const [isGenerating, setIsGenerating] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchApiKeys = async () => {
      try {
        const response = await fetch('/v1/keys');
        if (!response.ok) {
          throw new Error('Failed to fetch API keys');
        }
        const data = await response.json();
        const formattedKeys = data.keys.map((key: any) => ({
          id: key.id.toString(),
          key: key.key,
          createdAt: new Date(key.createdAt).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'long',
            day: 'numeric',
          }),
        }));
        setApiKeys(formattedKeys);
      } catch (err: any) {
        console.error('Error fetching API keys:', err);
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchApiKeys();
  }, []);

  const handleGenerateKey = async () => {
    setIsGenerating(true);
    setError(null);

    try {
      // No master key needed, this endpoint is now open for development
      const response = await fetch('/v1/keys', {
        method: 'POST',
      });

      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.error || 'Failed to generate key. Is the backend running?');
      }

      const data = await response.json();
      const newKey: ApiKey = {
        id: data.id.toString(),
        key: data.key,
        createdAt: new Date(data.createdAt).toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'long',
          day: 'numeric',
        }),
      };
      setApiKeys(prevKeys => [newKey, ...prevKeys]);

    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="p-6 bg-surface/50 rounded-xl border border-border">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6">
        <div>
            <h2 className="text-2xl font-bold text-text-primary">API Keys</h2>
            <p className="text-text-secondary mt-1">Generate new keys for your applications.</p>
        </div>
        <button
          onClick={handleGenerateKey}
          disabled={isGenerating}
          className="mt-4 sm:mt-0 bg-primary hover:bg-indigo-500 text-white font-bold py-2 px-4 rounded-lg transition-transform duration-200 ease-in-out hover:scale-105 disabled:bg-gray-500 disabled:cursor-not-allowed flex items-center justify-center"
        >
          {isGenerating ? (
              <>
                <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Generating...
              </>
            ) : (
              'Generate New Key'
            )}
        </button>
      </div>
      {error && <p className="text-red-400 mb-4 text-sm">{error}</p>}
      <div className="space-y-4">
        {isLoading ? (
          <div className="text-center py-10 px-4">
            <p className="text-text-secondary">Loading API keys...</p>
          </div>
        ) : apiKeys.length > 0 ? (
          apiKeys.map(apiKey => <ApiKeyRow key={apiKey.id} apiKey={apiKey} />)
        ) : (
          <div className="text-center py-10 px-4 border-2 border-dashed border-border rounded-lg">
            <p className="text-text-secondary">You haven't generated any API keys yet.</p>
            <p className="text-sm text-gray-500">Click "Generate New Key" to create your first one.</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default ApiKeyManager;