/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.goffish.tools.json;

import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.slice.SliceManager;

public class ResourceHolder {
    private static ResourceHolder ourInstance = new ResourceHolder();

    public static ResourceHolder getInstance() {
        return ourInstance;
    }

    private ResourceHolder() {
    }

    private SliceManager sliceManager;
    private IPartition partition;


    public SliceManager getSliceManager() {
        return sliceManager;
    }

    public void setSliceManager(SliceManager sliceManager) {
        this.sliceManager = sliceManager;
    }

    public IPartition getPartition() {
        return partition;
    }

    public void setPartition(IPartition partition) {
        this.partition = partition;
    }
}
