
import React, { useState } from 'react';
import ApiKeyManager from './components/ApiKeyManager';
import ApiTester from './components/ApiTester';
import ApiDocumentation from './components/ApiDocumentation';

const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState('tester');

  const renderTabContent = () => {
    switch (activeTab) {
      case 'keys':
        return <ApiKeyManager />;
      case 'tester':
        return <ApiTester />;
      case 'docs':
        return <ApiDocumentation />;
      default:
        return <ApiTester />;
    }
  };

  const TabButton = ({ tabId, children }: { tabId: string; children: React.ReactNode }) => (
    <button
      onClick={() => setActiveTab(tabId)}
      className={`px-4 py-2 text-sm font-medium rounded-md transition-colors duration-200 ${
        activeTab === tabId
          ? 'bg-primary text-white'
          : 'text-text-secondary hover:bg-surface hover:text-text-primary'
      }`}
    >
      {children}
    </button>
  );

  return (
    <div className="min-h-screen bg-background font-sans">
      <div className="container mx-auto px-4 py-8">
        <header className="text-center mb-12">
          <h1 className="text-4xl md:text-5xl font-bold text-text-primary mb-2">
            Video Download API Dashboard
          </h1>
          <p className="text-lg text-text-secondary max-w-2xl mx-auto">
            Manage keys, test endpoints, and integrate our powerful video download & merge service.
          </p>
        </header>

        <main>
          <div className="flex justify-center mb-8 border-b border-border pb-4">
            <nav className="flex space-x-2 md:space-x-4 bg-surface p-2 rounded-lg">
              {/* FIX: Explicitly pass children as a prop to fix TypeScript error where it wasn't being recognized. */}
              <TabButton tabId="tester" children="API Tester" />
              <TabButton tabId="keys" children="API Key Management" />
              <TabButton tabId="docs" children="Documentation" />
            </nav>
          </div>
          <div className="max-w-4xl mx-auto">
            {renderTabContent()}
          </div>
        </main>
        
        <footer className="text-center mt-16 text-text-secondary text-sm">
            <p>&copy; {new Date().getFullYear()} Video Processing Inc. All rights reserved.</p>
        </footer>
      </div>
    </div>
  );
};

export default App;
