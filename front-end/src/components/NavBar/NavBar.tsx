import Link from 'next/link';
import './style.css';

export const NavBar = () => {
    return (
        <div className="button-group">
            <button className="custom-button">
                <Link href={`#upload`} style={{ textAlign: 'center', width: '100%' }}>
                    <p>Upload</p>
                </Link>
            </button>
            <button className="custom-button">
                <Link href={`#dashboard`} style={{ textAlign: 'center', width: '100%' }}>
                    <p>Dashboard</p>
                </Link>
            </button>
            <button className="custom-button">
                <Link href={`#profile`} style={{ textAlign: 'center', width: '100%' }}>
                    <p>Profile</p>
                </Link>
            </button>
        </div>
    );
}

