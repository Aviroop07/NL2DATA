import React from 'react';
import { Box } from '@mui/material';
import MainPanel from './MainPanel';
import SidePanel from './SidePanel';

const Layout: React.FC = () => {
  return (
    <Box 
      sx={{ 
        height: '100vh', 
        width: '100vw', 
        display: 'flex', 
        bgcolor: 'background.default',
        overflow: 'hidden'
      }}
    >
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <MainPanel />
      </Box>
      <SidePanel />
    </Box>
  );
};

export default Layout;

