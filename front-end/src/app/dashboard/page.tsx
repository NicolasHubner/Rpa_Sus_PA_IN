import { Container } from "@/components/Container/Container";
import FileUpload from "@/components/DragDrop/DragDrop";
import { NavBar } from "@/components/NavBar/NavBar";


export default function Home() {
    return (
        <Container>
            <NavBar />
            <h1>asckos</h1>

            <FileUpload />
        </Container>
    )
}