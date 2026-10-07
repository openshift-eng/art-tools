import React from 'react';
import { createRoot } from 'react-dom/client';
import '@patternfly/react-core/dist/styles/base.css';
import App from './App.jsx';
import './styles.css';

let initialTheme = 'light';
try {
  if (window.localStorage.getItem('art-pipelines-theme') === 'dark') initialTheme = 'dark';
} catch {
  // Keep the light theme when browser storage is unavailable.
}
document.documentElement.dataset.theme = initialTheme;
document.documentElement.classList.toggle('pf-v6-theme-dark', initialTheme === 'dark');

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
