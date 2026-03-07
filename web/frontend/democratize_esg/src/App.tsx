import './App.css'
import {createBrowserRouter, RouterProvider} from "react-router-dom";
import RootLayout from "./RootLayout.tsx";
import LandingPage from "./pages/LandingPage.tsx";
import DataTable from "./pages/DataTable.tsx";
import DataVisualization from "./pages/DataVisualization.tsx";
import ReportPipeline from "./pages/ReportPipeline.tsx";


const router = createBrowserRouter([{
    path: '/',
    element: <RootLayout />,
    children: [
        {
            path: "/",
            element: <LandingPage />
        },
        {
            path: "data-table",
            element: <DataTable />
        },
        {
            path: "data-visualization",
            element: <DataVisualization />
        },
        {
            path: "report-pipeline",
            element: <ReportPipeline />
        },
    ]
}])

function App() {
  return (
    <RouterProvider router={router} />
  )
}

export default App
