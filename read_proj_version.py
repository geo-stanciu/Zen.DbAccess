import sys
import xml.etree.ElementTree as ET

def get_version_prefix_from_xml(xml_file_path):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Find the PropertyGroup element
        property_group = root.find('PropertyGroup')

        if property_group is not None:
            version_prefix_element = property_group.find('VersionPrefix')
            if version_prefix_element is not None:
                return version_prefix_element.text
        return None
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return None
    except FileNotFoundError:
        print(f"Error: File not found at {xml_file_path}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_proj_version.py <filename>")
        sys.exit(1) # Exit with an error code
    
    version = get_version_prefix_from_xml(sys.argv[1])
    
    if version.endswith(".0"):
        version = version[:-2]  # Remove the last two characters

    if version:
        sys.stdout.write(version)
    else:
        print("VersionPrefix not found or an error occurred.")
